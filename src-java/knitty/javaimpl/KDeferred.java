package knitty.javaimpl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Cleaner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import clojure.lang.AFn;
import clojure.lang.ExceptionInfo;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.RT;

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
                try {
                    logWarn(e, "unconsumed deferred in error state");
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    private final static class BindListener extends AListener {

        private final KDeferred dest;
        private final Object token;
        private final IFn valFn; // non-null
        private final IFn errFn; // nullable

        private BindListener(KDeferred dest, IFn valFn, IFn errFn, Object token) {
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

    private static class CdlListener extends AListener {
        final CountDownLatch cdl;

        CdlListener(CountDownLatch cdl) {
            this.cdl = cdl;
        }

        public void success(Object x) {
            cdl.countDown();
        }

        public void error(Object e) {
            cdl.countDown();
        }
    }

    private final static class KdChainListener extends AListener {

        private final KDeferred kd;
        private final Object token;

        public KdChainListener(KDeferred kd, Object token) {
            this.kd = kd;
            this.token = token;
        }

        public void success(Object x) {
            if (x instanceof IDeferred) {
                kd.chain(x, token);
            } else {
                kd.success(x, token);
            }
        }

        public void error(Object x) {
            if (x instanceof IDeferred) {
                kd.chain(x, token);
            } else {
                kd.error(x, token);
            }
        }
    }

    private final static class ChainListener implements IDeferredListener {

        private final KDeferred kd;
        private final Object token;

        public ChainListener(KDeferred kd, Object token) {
            this.kd = kd;
            this.token = token;
        }

        public Object onSuccess(Object x) {
            if (x instanceof IDeferred) {
                kd.chain(x, token);
            } else {
                kd.success(x, token);
            }
            return null;
        }

        public Object onError(Object x) {
            if (x instanceof IDeferred) {
                kd.chain(x, token);
            } else {
                kd.error(x, token);
            }
            return null;
        }

        public IFn successCallback() {
            return new AFn() {
                public Object invoke(Object x) {
                    return onSuccess(x);
                }
            };
        }

        public IFn errorCallback() {
            return new AFn() {
                public Object invoke(Object x) {
                    return onError(x);
                }
            };
        }
    }
    private final static class RevokeListener extends AListener {

        private final IDeferred d;
        private final IFn canceller;
        private final IFn errCallback;

        public RevokeListener(IDeferred d, IFn canceller, IFn errCallback) {
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

    private final static class KdRevokeListener extends AListener {

        private final KDeferred d;

        public KdRevokeListener(KDeferred d) {
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
    private static final VarHandle OWNED;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            STATE = l.findVarHandle(KDeferred.class, "state", Byte.TYPE);
            OWNED = l.findVarHandle(KDeferred.class, "owned", Boolean.TYPE);
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
    boolean owned;
    boolean revokable;
    private Object value;
    private Object token;
    private AListener lss;
    private IPersistentMap meta;
    private ErrorLeakDetector eld;

    private void fireEld(Object err) {
        if (!(err instanceof RevokeException)) {
            ErrorLeakDetector eld = new ErrorLeakDetector(err);
            this.eld = eld;
            ELD_CLEANER.register(this, eld);
        }
    }

    public void clearEld() {
        ErrorLeakDetector eld = this.eld;
        if (eld != null) {
            eld.err = null;
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

    final boolean owned() {
        return (boolean) OWNED.getAndSet(this, true);
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
        return success(x, null);
    }

    public Object success(Object x, Object token) {
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

                    for (; node != null; node = node.next) {
                        try {
                            node.success(x);
                        } catch (ArithmeticException e) {
                            logError(e, "error in deferred handler");
                        }
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
        return error(x, null);
    }

    public Object error(Object x, Object token) {
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
                    this.state = STATE_ERRR;

                    for (; node != null; node = node.next) {
                        try {
                            node.error(x);
                        } catch (Throwable e) {
                            logError(e, "error in deferred handler");
                        }
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

    public boolean listen0(AListener ls) {
        return listen0(this.state, ls);
    }

    private boolean listen0(byte s, AListener ls) {
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
                case STATE_LOCK:
                    Thread.onSpinWait();
                    s = this.state;
                    continue;
                case STATE_SUCC:
                    return false;
                case STATE_ERRR:
                    return false;
            }
        }
    }

    public boolean listen(AListener ls) {
        byte s = this.state;
        while (true) {
            switch (s) {
                case STATE_SUCC:
                    ls.success(value);
                    return false;
                case STATE_ERRR:
                    this.clearEld();
                    ls.error(value);
                    return false;
                default:
                    if (this.listen0(s, ls)) {
                        return true;
                    } else {
                        s = this.state;
                        continue;
                    }
            }
        }
    }

    public Object addListener(Object lss) {
        IDeferredListener ls = (IDeferredListener) lss;
        while (true) {
            switch (this.state) {
                case STATE_LSTN:
                case STATE_LOCK:
                    if (this.listen0(new AListener.Dl(ls))) {
                        return Boolean.TRUE;
                    }
                case STATE_SUCC:
                    ls.onSuccess(value);
                    return Boolean.FALSE;
                case STATE_ERRR:
                    this.clearEld();
                    ls.onError(value);
                    return Boolean.FALSE;
            }
        }
    }

    public Object onRealized(Object onSucc, Object onErrr) {
        IFn onSuc = (IFn) onSucc;
        IFn onErr = (IFn) onErrr;
        while (true) {
            switch (this.state) {
                case STATE_LSTN:
                case STATE_LOCK:
                    if (this.listen0(new AListener.Fn(onSuc, onErr))) {
                        return Boolean.TRUE;
                    }
                case STATE_SUCC:
                    onSuc.invoke(value);
                    return Boolean.FALSE;
                case STATE_ERRR:
                    this.clearEld();
                    onErr.invoke(value);
                    return Boolean.FALSE;
            }
        }
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

    public Object errorValue(Object fallback) {
        if (this.state == STATE_ERRR) {
            this.clearEld();
            return value;
        }
        return fallback;
    }

    public Object unwrap() {
        return this.state == STATE_SUCC ? value : this;
    }

    public Object get() {
        byte s = this.state;
        switch (s) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                this.clearEld();
                throwErr(value);
            default:
                throw new IllegalStateException("kdeferred is not realized");
        }
    }

    private CountDownLatch acquireCountdDownLatch() {
        CountDownLatch cdl = new CountDownLatch(1);
        this.listen(new CdlListener(cdl));
        return cdl;
    }

    public Object deref(long ms, Object timeoutValue) {

        switch (this.state) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                this.clearEld();
                throwErr(value);
        }

        if (ms <= 0) {
            return timeoutValue;
        }

        try {
            acquireCountdDownLatch().await(ms, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throwErr(e);
        }

        switch (this.state) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                this.clearEld();
                throwErr(value);
        }

        return timeoutValue;
    }

    public Object deref() {

        switch (this.state) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                this.clearEld();
                throwErr(value);
        }

        try {
            acquireCountdDownLatch().await();
        } catch (InterruptedException e) {
            throwErr(e);
        }

        switch (this.state) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                this.clearEld();
                throwErr(value);
        }

        throw new IllegalStateException();
    }

    public void revokeTo(KDeferred d) {
        if (d.revokable && !d.realized()) {
            this.listen(new KdRevokeListener(d));
        }
    }

    public void chain(Object x, Object token) {
        while (true) {
            if (x instanceof IDeferred) {
                Object xx = ((IDeferred) x).successValue(x);
                if (x == xx) {
                    break;
                }
                x = xx;
            } else {
                this.success(x, token);
                return;
            }
        }
        this.chain0((IDeferred) x, token);
    }

    private void chain0(IDeferred x, Object token) {
        if (x instanceof KDeferred) {
            KDeferred xx = (KDeferred) x;
            if (!xx.listen0(new KdChainListener(this, token))) {
                switch (xx.state) {
                    case STATE_SUCC:
                        this.success(xx.value, token); break;
                    case STATE_ERRR:
                        xx.clearEld();
                        this.error(xx.value, token); break;
                }
            } else if (xx.revokable) {
                this.listen(new KdRevokeListener(xx));
            }
        } else {
            ChainListener ls = new ChainListener(this, token);
            if (x instanceof IMutableDeferred) {
                ((IMutableDeferred) x).addListener(ls);
            } else {
                ((IDeferred) x).onRealized(ls.successCallback(), ls.errorCallback());
            }
        }
    }

    public KDeferred bind(IFn valFn, IFn errFn, Object token) {
        while (true) {
            switch (this.state) {
                case STATE_LSTN:
                    KDeferred dest = new KDeferred(token);
                    if (this.listen0(new BindListener(dest, valFn, errFn, token))) {
                        return dest;
                    }
                    continue;
                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;
                case STATE_SUCC:
                    return wrap(valFn.invoke(value));
                case STATE_ERRR:
                    if (errFn == null) {
                        return this;
                    } else {
                        this.clearEld();
                        return wrap(errFn.invoke(value));
                    }

            }
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
        d.fireEld(e);
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
            kd.listen0(new RevokeListener(d, canceller, errCallback));
            kd.chain(d, null);
            return kd;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwErr(Object err) throws T {
        if (err instanceof Throwable) {
            throw (T) err;
        } else {
            throw new ExceptionInfo(
                "invalid error object",
                    PersistentArrayMap.EMPTY.assoc(Keyword.find("error"), err)
            );
        }
    }
}