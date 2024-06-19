package knitty.javaimpl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Cleaner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import clojure.lang.AFn;
import clojure.lang.ExceptionInfo;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.RT;
import clojure.lang.Util;
import clojure.lang.Var;
import manifold.deferred.IDeferred;
import manifold.deferred.IDeferredListener;
import manifold.deferred.IMutableDeferred;

public final class KDeferred
        implements
        clojure.lang.IDeref,
        clojure.lang.IBlockingDeref,
        clojure.lang.IPending,
        clojure.lang.IReference,
        IDeferred,
        IMutableDeferred {

    public static final BlockingQueue<Object> ELD_LEAKED_ERRORS = new ArrayBlockingQueue<>(128);

    public static final Thread ELD_LOGGER = new Thread(new Runnable() {
        public void run() {
            while (!Thread.interrupted()) {
                Object e;
                try {
                    e = ELD_LEAKED_ERRORS.take();
                } catch (InterruptedException ex) {
                    return;
                }
                try {
                    logWarn(e, "unconsumed deferred in error state");
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }
    }, "knitty-error-leak-logger");

    static {
        ELD_LOGGER.setDaemon(true);
        ELD_LOGGER.start();
    }

    private static final Cleaner ELD_CLEANER =
        Cleaner.create(r -> new Thread(r, "knitty-error-leak-detector"));

    private static class ErrorLeakDetector implements Runnable {

        volatile Object err;

        public ErrorLeakDetector(Object err) {
            this.err = err;
        }

        public void run() {
            Object e = this.err;
            if (e != null) {
                this.err = null;
                ELD_LEAKED_ERRORS.offer(e);
            }
        }
    }

    private final static class Bind extends AListener {

        private final KDeferred dest;
        private final Object token;
        private final IFn valFn; // non-null
        private final IFn errFn; // nullable

        private Bind(KDeferred dest, IFn valFn, IFn errFn, Object token) {
            this.dest = dest;
            this.token = token;
            this.valFn = valFn;
            this.errFn = errFn;
        }

        public void success(Object x) {
            Object t;
            try {
                t = valFn.invoke(x);
            } catch (Throwable e) {
                dest.error(e, token);
                return;
            }
            if (t instanceof IDeferred) {
                dest.chain(t, token);
            } else {
                dest.success(t, token);
            }
        }

        public void error(Object e) {
            if (errFn == null) {
                dest.error(e, token);
            } else {
                Object t;
                try {
                    t = errFn.invoke(e);
                } catch (Throwable e1) {
                    dest.error(e1, token);
                    return;
                }
                if (t instanceof IDeferred) {
                    dest.chain(t, token);
                } else {
                    dest.success(t, token);
                }
            }
        }
    }

    private static class Cdl extends AListener {
        final CountDownLatch cdl;

        Cdl(CountDownLatch cdl) {
            this.cdl = cdl;
        }

        public void success(Object x) {
            cdl.countDown();
        }

        public void error(Object e) {
            cdl.countDown();
        }
    }

    private final static class Chain extends AListener {

        private final KDeferred kd;
        private final Object token;

        public Chain(KDeferred kd, Object token) {
            this.kd = kd;
            this.token = token;
        }

        public void success(Object x) {
            kd.success(x, token);
        }

        public void error(Object x) {

            kd.error(x, token);
        }
    }

    private final static class ChainSuccess extends AFn {

        private final KDeferred kd;
        private final Object token;

        public ChainSuccess(KDeferred kd, Object token) {
            this.kd = kd;
            this.token = token;
        }

        public Object invoke(Object x) {
            kd.success(x, token);
            return null;
        }
    }

    private final static class ChainError extends AFn {

        private final KDeferred kd;
        private final Object token;

        public ChainError(KDeferred kd, Object token) {
            this.kd = kd;
            this.token = token;
        }

        public Object invoke(Object x) {
            kd.error(x, token);
            return null;
        }
    }

    private final static class Revoke extends AListener {

        private final IDeferred d;
        private final IFn canceller;
        private final IFn errCallback;

        public Revoke(IDeferred d, IFn canceller, IFn errCallback) {
            this.d = d;
            this.canceller = canceller;
            this.errCallback = errCallback;
        }

        public void success(Object x) {
            if (!d.realized()) {
                canceller.invoke();
            }
        }

        public void error(Object x) {
            if (!d.realized()) {
                canceller.invoke();
            }
            if (errCallback != null) {
                errCallback.invoke(x);
            }
        }
    }

    private final static class KdRevoke extends AListener {

        private final KDeferred d;

        public KdRevoke(KDeferred d) {
            this.d = d;
        }

        public void success(Object x) {
            if (!d.realized()) {
                d.error(RevokeException.DEFERRED_REVOKED, d.token);
            }
        }

        public void error(Object x) {
            if (!d.realized()) {
                d.error(new RevokeException((Throwable) x), d.token);
            }
        }
    }

    static final byte STATE_LSTN = 0;
    static final byte STATE_SUCC = 1;
    static final byte STATE_ERRR = 2;
    static final byte STATE_LOCK = 4;
    static final byte STATE_DONE_MASK = STATE_SUCC | STATE_ERRR;

    private static final VarHandle STATE;
    private static final VarHandle FREE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            STATE = l.findVarHandle(KDeferred.class, "state", Byte.TYPE);
            FREE = l.findVarHandle(KDeferred.class, "free", Boolean.TYPE);
        } catch (ReflectiveOperationException var1) {
            throw new ExceptionInInitializerError(var1);
        }
    }

    private static volatile IFn LOG_EXCEPTION = new AFn() {
        public Object invoke(Object err, Object msg) {
            System.err.printf("%s: %s", msg, err);
            if (err instanceof Throwable) {
                ((Throwable) err).printStackTrace();
            }
            return null;
        };
    };

    public static void setExceptionLogFn(IFn f) {
        LOG_EXCEPTION = f;
    }

    volatile byte state;
    boolean free = true;
    boolean revokable;
    private Object value;
    private Object token;
    private AListener lss;
    private IPersistentMap meta;
    private ErrorLeakDetector eld;

    private void fireError(Object err) {
        if (!(err instanceof CancellationException)) {
            ErrorLeakDetector x = new ErrorLeakDetector(new Exception((Throwable) err));
            this.eld = x;
            ELD_CLEANER.register(this, x);
        }
    }

    public final void consumeError() {
        ErrorLeakDetector x = this.eld;
        if (x != null) {
            x.err = null;
            this.eld = null;
        }
    }

    KDeferred() {
    }

    KDeferred(Object token) {
        this.token = token;
    }

    KDeferred(byte state, Object value) {
        this.value = value;
        this.state = state;
    }

    public final boolean own() {
        return free && (boolean) FREE.getAndSet(this, false);
    }

    public synchronized IPersistentMap meta() {
        return meta;
    }

    public synchronized IPersistentMap alterMeta(IFn alter, ISeq args) {
        this.meta = (IPersistentMap) alter.applyTo(RT.listStar(meta, args));
        return this.meta;
    }

    public synchronized IPersistentMap resetMeta(IPersistentMap m) {
        this.meta = m;
        return m;
    }

    static void logWarn(Object err, String msg) {
        try {
            LOG_EXCEPTION.invoke(false, err, msg);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    static void logError(Object err, String msg) {
        try {
            LOG_EXCEPTION.invoke(true, err, msg);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public Object success(Object x) {
        if (this.state == STATE_LSTN && this.lss == null && STATE.compareAndSet(this, STATE_LSTN, STATE_LOCK)) {
            if (this.lss == null && this.token == null) {
                this.value = x;
                this.state = STATE_SUCC;
                return Boolean.TRUE;
            } else {
                this.state = STATE_LSTN;
            }
        }
        return success0(x, token);
    }

    public Object success(Object x, Object token) {
        if (this.state == STATE_LSTN && this.lss == null && STATE.compareAndSet(this, STATE_LSTN, STATE_LOCK)) {
            if (this.lss == null && this.token == token) {
                this.value = x;
                this.state = STATE_SUCC;
                return Boolean.TRUE;
            } else {
                this.state = STATE_LSTN;
            }
        }
        return success0(x, token);
    }

    public Boolean success0(Object x, Object token) {
        byte s = this.state;
        while (true) {
            switch (s) {

                case STATE_LSTN:

                    byte s0 = (byte) STATE.compareAndExchange(this, s, STATE_LOCK);
                    if (s != s0) {
                        s = s0;
                        continue;
                    }
                    if (token != this.token) {
                        this.state = s;
                        throw new IllegalStateException("invalid claim-token");
                    }

                    this.value = x;
                    AListener node = this.lss;
                    this.lss = null;
                    this.state = STATE_SUCC;

                    if (node != null) {
                        Object frame = Var.getThreadBindingFrame();
                        for (; node != null; node = node.next) {
                            try {
                                node.resetFrame();
                                node.success(x);
                            } catch (Throwable e) {
                                logError(e, String.format("error in deferred success-handler: %s", node));
                            }
                        }
                        Var.resetThreadBindingFrame(frame);
                    }

                    return Boolean.TRUE;

                case STATE_LOCK:
                    Thread.onSpinWait();
                    s = this.state;
                    continue;

                case STATE_SUCC:
                    return Boolean.FALSE;

                case STATE_ERRR:
                    return Boolean.FALSE;
            }
        }
    }

    public Object error(Object x) {
        if (this.state == STATE_LSTN && this.lss == null && STATE.compareAndSet(this, STATE_LSTN, STATE_LOCK)) {
            if (this.lss == null && this.token == null) {
                this.value = x;
                this.fireError(x);
                this.state = STATE_ERRR;
                return Boolean.TRUE;
            } else {
                this.state = STATE_LSTN;
            }
        }
        return error0(x, token);
    }

    public Object error(Object x, Object token) {
        if (this.state == STATE_LSTN && this.lss == null && STATE.compareAndSet(this, STATE_LSTN, STATE_LOCK)) {
            if (this.lss == null && this.token == token) {
                this.value = x;
                this.fireError(x);
                this.state = STATE_ERRR;
                return Boolean.TRUE;
            } else {
                this.state = STATE_LSTN;
            }
        }
        return error0(x, token);
    }

    public Boolean error0(Object x, Object token) {
        byte s = this.state;
        while (true) {
            switch (s) {

                case STATE_LSTN:
                    byte s0 = (byte) STATE.compareAndExchange(this, s, STATE_LOCK);
                    if (s != s0) {
                        s = s0;
                        continue;
                    }
                    if (token != this.token) {
                        this.state = s;
                        throw new IllegalStateException("invalid claim-token");
                    }

                    this.value = x;
                    AListener node = this.lss;
                    this.lss = null;
                    if (node == null) {
                        this.fireError(x);
                    }

                    this.state = STATE_ERRR;

                    if (node != null) {
                        this.consumeError();
                        Object frame = Var.getThreadBindingFrame();
                        for (; node != null; node = node.next) {
                            try {
                                node.resetFrame();
                                node.error(x);
                            } catch (Throwable e) {
                                logError(e, String.format("error in deferred error-handler: %s", node));
                            }
                        }
                        Var.resetThreadBindingFrame(frame);
                    }

                    return Boolean.TRUE;

                case STATE_LOCK:
                    Thread.onSpinWait();
                    s = this.state;
                    continue;

                case STATE_SUCC:
                    return Boolean.FALSE;

                case STATE_ERRR:
                    return Boolean.FALSE;
            }
        }
    }

    public boolean listen0(IFn onSuc, IFn onErr) {
        byte s = this.state;
        return (s & STATE_DONE_MASK) == 0 && listen0(s, new AListener.Fn(onSuc, onErr));
    }

    public boolean listen0(IDeferredListener ls) {
        byte s = this.state;
        return (s & STATE_DONE_MASK) == 0 && listen0(s, new AListener.Dl(ls));
    }

    boolean listen0(AListener ls) {
        return listen0(this.state, ls);
    }

    private boolean listen0(byte s, AListener ls) {
        if (ls.next != null) {
            throw new IllegalArgumentException("listener is already used for another deferred");
        }
        while (true) {
            switch (s) {
                case STATE_LSTN:
                    if ((s = (byte) STATE.compareAndExchange(this, s, STATE_LOCK)) == STATE_LSTN) {
                        ls.next = this.lss;
                        this.lss = ls;
                        this.state = STATE_LSTN;
                        return true;
                    }
                    continue;
                case STATE_SUCC:
                    return false;
                case STATE_ERRR:
                    return false;
                case STATE_LOCK:
                    s = this.state;
                    Thread.onSpinWait();
            }
        }
    }

    public void listen(IFn onSuc, IFn onErr) {
        if (this.listen0(onSuc, onErr)) {
            return;
        }
        if (this.state == STATE_SUCC) {
            onSuc.invoke(this.value);
        } else {
            this.consumeError();
            onErr.invoke(this.value);
        }
    }

    public void listen(IDeferredListener ls) {
        if (this.listen0(ls)) {
            return;
        }
        if (this.state == STATE_SUCC) {
            ls.onSuccess(this.value);
        } else {
            this.consumeError();
            ls.onError(this.value);
        }
    }

    void listen(AListener ls) {
        if (this.listen0(ls)) {
            return;
        }
        Object frame = Var.getThreadBindingFrame();
        try {
            ls.resetFrame();
            if (this.state == STATE_SUCC) {
                ls.success(value);
            } else {
                this.consumeError();
                ls.error(value);
            }
        } finally {
            Var.resetThreadBindingFrame(frame);
        }
    }

    public Object addListener(Object lss) {
        IDeferredListener ls = (IDeferredListener) lss;
        if (this.listen0(ls)) {
            return Boolean.TRUE;
        }

        if (this.state == STATE_SUCC) {
            ls.onSuccess(value);
        } else {
            this.consumeError();
            ls.onError(value);
        }
        return Boolean.FALSE;
    }

    public Object onRealized(Object onSucc, Object onErrr) {
        IFn onSuc = (IFn) onSucc;
        IFn onErr = (IFn) onErrr;

        if (this.listen0(onSuc, onErr)) {
            return Boolean.TRUE;
        }

        if (this.state == STATE_SUCC) {
            onSuc.invoke(value);
        } else {
            this.consumeError();
            onErr.invoke(value);
        }
        return Boolean.FALSE;
    }

    public Object cancelListener(Object listener) {
        throw new UnsupportedOperationException("cancelling of listeners is not supported");
    }

    public Object claim() {
        if (token == null) {
            Object t = new Object();
            return claim(t) ? t : null;
        }
        return null;
    }

    public boolean claim(Object token) {
        while (true) {
            byte s = this.state;
            switch (s) {
                case STATE_LSTN:
                    byte nst = (byte) STATE.compareAndExchange(this, s, STATE_LOCK);
                    if (nst != s) {
                        s = nst;
                        continue;
                    }
                    if (this.token == null) {
                        this.token = token;
                        this.state = s;
                        return true;
                    } else {
                        this.state = s;
                        return false;
                    }
                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;
                default:
                    return false;
            }
        }
    }

    public Object executor() {
        return null;
    }

    public boolean realized() {
        return (this.state & STATE_DONE_MASK) != 0;
    }

    public boolean isRealized() {
        return (this.state & STATE_DONE_MASK) != 0;
    }

    public Object successValue(Object fallback) {
        return this.state == STATE_SUCC ? value : fallback;
    }

    @SuppressWarnings("unchecked")
    private <T extends Throwable> void throwErr() throws T {
        this.consumeError();
        Object err = value;
        if (err instanceof Throwable) {
            throw (T) err;
        } else {
            throw new ExceptionInfo(
                "invalid error object",
                    PersistentArrayMap.EMPTY.assoc(Keyword.find("error"), err)
            );
        }
    }

    public Object errorValue(Object fallback) {
        if (this.state == STATE_ERRR) {
            this.consumeError();
            return value;
        }
        return fallback;
    }

    public Object unwrap() {
        return this.state == STATE_SUCC ? value : this;
    }

    private Object getErr() {
        if (this.state == STATE_ERRR) {
            throwErr();
        }
        throw new IllegalStateException("kdeferred is not realized");
    }

    public final Object get() {
        return (this.state == STATE_SUCC) ? value : getErr();
    }

    private CountDownLatch acquireCountdDownLatch() {
        CountDownLatch cdl = new CountDownLatch(1);
        this.listen(new Cdl(cdl));
        return cdl;
    }

    public Object deref(long ms, Object timeoutValue) {

        switch (this.state) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                throwErr();
        }

        if (ms <= 0) {
            return timeoutValue;
        }

        try {
            acquireCountdDownLatch().await(ms, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw Util.sneakyThrow(e);
        }

        switch (this.state) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                throwErr();
        }

        return timeoutValue;
    }

    public Object deref() {

        switch (this.state) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                throwErr();
        }

        try {
            acquireCountdDownLatch().await();
        } catch (InterruptedException e) {
            throw Util.sneakyThrow(e);
        }

        switch (this.state) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                throwErr();
        }

        throw new IllegalStateException();
    }

    public void revokeTo(KDeferred d) {
        if (d.revokable && !d.realized()) {
            this.listen(new KdRevoke(d));
        }
    }

    public void chain(Object x, Object token) {
        if (x instanceof IDeferred) {
            Object xx = ((IDeferred) x).successValue(x);
            if (x == xx) {
                this.chain0((IDeferred) x, token);
                return;
            } else {
                x = xx;
            }
        }
        this.success(x, token);
    }

    private void chain0(IDeferred x, Object token) {
        if (x instanceof KDeferred) {
            KDeferred xx = (KDeferred) x;
            if (xx.listen0(new Chain(this, token))) {
                this.revokeTo(xx);
            } else {
                switch (xx.state) {
                    case STATE_SUCC:
                        this.success(xx.value, token); break;
                    case STATE_ERRR:
                        xx.consumeError();
                        this.error(xx.value, token); break;
                }
            }
        } else {
            x.onRealized(new ChainSuccess(this, token), new ChainError(this, token));
        }
    }

    public KDeferred bind(IFn valFn, IFn errFn, Object token, Executor executor) {
        KDeferred dest = new KDeferred(token);
        this.listen(AListener.viaExecutor(new Bind(dest, valFn, errFn, token), executor));
        return dest;
    }

    public KDeferred bind(IFn valFn, IFn errFn, Object token) {
        byte s;
        loop:
        while (true) {
            switch ((s = this.state)) {
                case STATE_LSTN:
                    KDeferred dest = new KDeferred(token);
                    if (this.listen0(s, new Bind(dest, valFn, errFn, token))) {
                        return dest;
                    }
                    continue;
                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;
                case STATE_SUCC:
                    break loop;
                case STATE_ERRR:
                    if (errFn == null) {
                        return this;
                    } else {
                        this.consumeError();
                        valFn = errFn;
                    }
                    break loop;
            }
        }
        try {
            return wrap(valFn.invoke(value));
        } catch (Throwable e) {
            return wrapErr(e);
        }
    }

    public static KDeferred create() {
        return new KDeferred();
    }

    public static KDeferred create(Object token) {
        return new KDeferred(token);
    }

    public static KDeferred wrapErr(Object e) {
        KDeferred d = new KDeferred(STATE_ERRR, e);
        d.fireError(e);
        return d;
    }

    public static KDeferred wrapVal(Object x) {
        return new KDeferred(STATE_SUCC, x);
    }

    public static KDeferred wrapDeferred(IDeferred x) {
        if (x instanceof KDeferred) {
            return (KDeferred) x;
        } else {
            KDeferred d = new KDeferred(x);
            d.chain(x, x);
            return d;
        }
    }

    public static KDeferred wrap(Object x) {
        if (x instanceof IDeferred) {
            return wrapDeferred((IDeferred) x);
        } else {
            return new KDeferred(STATE_SUCC, x);
        }
    }

    public static KDeferred revoke(IDeferred d, IFn canceller, IFn errCallback) {
        if (d.realized()) {
            return wrapDeferred(d);
        } else {
            KDeferred kd = new KDeferred();
            kd.revokable = true;
            kd.listen0(new Revoke(d, canceller, errCallback));
            kd.chain(d, null);
            return kd;
        }
    }
}