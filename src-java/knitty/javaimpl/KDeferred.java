package knitty.javaimpl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import clojure.lang.AFn;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.ISeq;
import clojure.lang.RT;
import clojure.lang.Util;

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

    private final static class BindListener extends AListener {

        private final KDeferred dest;
        private final Object token;
        private final AFn valFn; // non-null
        private final AFn errFn; // nullable

        private BindListener(KDeferred dest, AFn valFn, AFn errFn, Object token) {
            this.dest = dest;
            this.token = token;
            this.valFn = valFn;
            this.errFn = errFn;
        }

        public Object onSuccess(Object x) {
            Object t;
            try {
                t = valFn.invoke(x);
            } catch (Throwable e) {
                dest.error0(dest.state, e, token);
                return null;
            }
            if (t instanceof IDeferred) {
                dest.chain0((IDeferred) t, token);
            } else {
                dest.success0(dest.state, t, token);
            }
            return null;
        }

        public Object onError(Object e) {
            if (errFn == null) {
                dest.error(e, token);
                return null;
            } else {
                Object t;
                try {
                    t = errFn.invoke(e);
                } catch (Throwable e1) {
                    dest.error0(dest.state, e1, token);
                    return null;
                }
                if (t instanceof IDeferred) {
                    dest.chain0((IDeferred) t, token);
                } else {
                    dest.success0(dest.state, t, token);
                }
                return null;
            }
        }
    }

    private static class CdlListener extends AListener {
        final CountDownLatch cdl;

        CdlListener(CountDownLatch cdl) {
            this.cdl = cdl;
        }

        public Object onSuccess(Object x) {
            cdl.countDown();
            return null;
        }

        public Object onError(Object e) {
            cdl.countDown();
            return null;
        }
    }

    private final static class ChainListener extends AListener {

        private final KDeferred kd;
        private final Object token;

        public ChainListener(KDeferred kd, Object token) {
            this.kd = kd;
            this.token = token;
        }

        public Object onSuccess(Object x) {
            if (x instanceof IDeferred) {
                kd.chain0((IDeferred) x, token);
            } else {
                kd.success0(kd.state, x, token);
            }
            return null;
        }

        public Object onError(Object x) {
            if (x instanceof IDeferred) {
                kd.chain0((IDeferred) x, token);
            } else {
                kd.error0(kd.state, x, token);
            }
            return null;
        }

        public AFn successCallback() {
            return new AFn() {
                public Object invoke(Object x) {
                    return onSuccess(x);
                }
            };
        }

        public AFn errorCallback() {
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

        public Object onSuccess(Object x) {
            if (!d.realized()) {
                canceller.invoke();
            }
            return null;
        }

        public Object onError(Object x) {
            if (!d.realized()) {
                canceller.invoke();
            }
            if (errCallback != null) {
                errCallback.invoke(x);
            }
            return null;
        }
    }

    private final static class KdRevokeListener extends AListener {

        private final KDeferred d;

        public KdRevokeListener(KDeferred d) {
            this.d = d;
        }

        public Object onSuccess(Object x) {
            if (!d.realized()) {
                d.error0(d.state, RevokeException.INSTANCE, d.token);
            }
            return null;
        }

        public Object onError(Object x) {
            if (!d.realized()) {
                d.error0(d.state, new RevokeException((Throwable) x), d.token);
            }
            return null;
        }
    }

    private static final class Listeners {

        public final IDeferredListener ls0;
        public IDeferredListener ls1;
        public IDeferredListener ls2;
        public IDeferredListener ls3;
        public int pos;
        public Listeners next;

        public Listeners(IDeferredListener x) {
            this.ls0 = x;
        }

        public IDeferredListener get(int i) {
            switch (i) {
                case 0:
                    return this.ls0;
                case 1:
                    return this.ls1;
                case 2:
                    return this.ls2;
                case 3:
                    return this.ls3;
                default:
                    throw new IllegalStateException();
            }
        }

        public Listeners push(IDeferredListener ls) {
            if (this.pos == 3) {
                return this.next = new Listeners(ls);
            } else {
                switch (++this.pos) {
                    case 1:
                        this.ls1 = ls;
                        break;
                    case 2:
                        this.ls2 = ls;
                        break;
                    case 3:
                        this.ls3 = ls;
                        break;
                    default:
                        throw new IllegalStateException();
                }
                return this;
            }
        }
    }

    static final byte STATE_INIT = 0;
    static final byte STATE_SUCC = 1;
    static final byte STATE_ERRR = 2;
    static final byte STATE_LSTN = 4;
    static final byte STATE_LOCK = 8;
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

    private static volatile IFn LOG_EXCEPTION;

    public static void setExceptionLogFn(IFn f) {
        LOG_EXCEPTION = f;
    }

    volatile byte state;
    boolean owned;
    boolean revokable;
    private Object value;
    private Object token;
    private Listeners lcFirst;
    private Listeners lcLast;
    private IPersistentMap meta;

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

    static void logException(Throwable e0) {
        try {
            IFn log = LOG_EXCEPTION;
            if (log != null) {
                log.invoke(e0);
            } else {
                e0.printStackTrace();
                ;
            }
        } catch (Throwable e1) {
            e0.printStackTrace();
            e1.printStackTrace();
        }
    }

    public Object success(Object x) {
        byte s = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK);
        if (s == STATE_INIT && this.token == null) {
            this.value = x;
            this.state = STATE_SUCC;
            return true;
        } else {
            return success0(s, x, null);
        }
    }

    public Object success(Object x, Object token) {
        byte s = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK);
        if (s == STATE_INIT && this.token == token) {
            this.value = x;
            this.state = STATE_SUCC;
            return true;
        } else {
            return success0(s, x, token);
        }
    }

    private Object success0(byte s, Object x, Object token) {
        while (true) {
            switch (s) {

                case STATE_LSTN:
                    s = (byte) STATE.compareAndExchange(this, STATE_LSTN, STATE_LOCK);
                    if (s != STATE_LSTN)
                        continue;
                    if (token != this.token) {
                        this.state = STATE_LSTN;
                        throw new IllegalStateException("invalid claim-token");
                    }

                    Listeners node = this.lcFirst;
                    this.value = x;
                    this.lcLast = null;
                    this.lcFirst = null;
                    this.state = STATE_SUCC;

                    for (; node != null; node = node.next) {
                        for (int i = 0; i <= node.pos; ++i) {
                            IDeferredListener ls = node.get(i);
                            try {
                                ls.onSuccess(value);
                                                        } catch (StackOverflowError e) {
                                throw e;
                            } catch (Throwable e) {
                                logException(e);
                            }
                        }
                    }
                    return true;

                case STATE_SUCC:
                    return false;

                case STATE_ERRR:
                    return false;

                case STATE_INIT:
                    s = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK);
                    if (s != STATE_INIT)
                        continue;
                    if (token != this.token) {
                        this.state = STATE_INIT;
                        throw new IllegalStateException("invalid claim-token");
                    }

                    this.value = x;
                    this.state = STATE_SUCC;
                    return true;

                case STATE_LOCK:
                    Thread.onSpinWait();
                    s = this.state;
                    continue;
            }
        }
    }

    public Object error(Object x) {
        byte s = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK);
        if (s == STATE_INIT && this.token == null) {
            this.value = x;
            this.state = STATE_ERRR;
            return true;
        } else {
            return this.error0(s, x, null);
}
    }

    public Object error(Object x, Object token) {
        byte s = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK);
        if (s == STATE_INIT && this.token == token) {
            this.value = x;
            this.state = STATE_ERRR;
            return true;
        } else {
                            return this.error0(s, x, token);
                    }
    }

    private Object error0(byte s, Object x, Object token) {
        while (true) {
            switch (s) {

                case STATE_LSTN:
                    s = (byte) STATE.compareAndExchange(this, STATE_LSTN, STATE_LOCK);
                    if (s != STATE_LSTN)
                        continue;

                    if (token != this.token) {
                        this.state = STATE_LSTN;
                        throw new IllegalStateException("invalid claim-token");
                    }

                    Listeners node = this.lcFirst;
                    this.value = x;
                    this.lcLast = null;
                    this.lcFirst = null;
                    this.state = STATE_ERRR;

                    for (; node != null; node = node.next) {
                        for (int i = 0; i <= node.pos; ++i) {
                            IDeferredListener ls = node.get(i);
                            try {
                                ls.onError(value);
                                                        } catch (StackOverflowError e) {
                                throw e;
                            } catch (Throwable e) {
                                logException(e);
                            }
                        }
                    }
                    return true;

                case STATE_SUCC:
                    return false;

                case STATE_ERRR:
                    return false;

                case STATE_INIT:
                    s = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK);
                    if (s != STATE_INIT)
                        continue;

                    if (token != this.token) {
                        this.state = STATE_INIT;
                        throw new IllegalStateException("invalid claim-token");
                    }

                    this.value = x;
                    this.state = STATE_ERRR;
                    return true;

                case STATE_LOCK:
                    Thread.onSpinWait();
                    s = this.state;
                    continue;
            }
        }
    }

    private boolean pushListener1(IDeferredListener x) {

        byte state;
        while ((state = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK)) == STATE_LOCK)
            Thread.onSpinWait();

        if (state == STATE_INIT) {
            this.lcFirst = this.lcLast = new Listeners(x);
            this.state = STATE_LSTN;
            return true;
        } else {
            return false;
        }
    }

    private boolean pushListenerX(IDeferredListener x) {

        byte state;
        while ((state = (byte) STATE.compareAndExchange(this, STATE_LSTN, STATE_LOCK)) == STATE_LOCK)
            Thread.onSpinWait();

        if (state == STATE_LSTN) {
            this.lcLast = this.lcLast.push(x);
            this.state = (STATE_LSTN);
            return true;
        } else {
            return false;
        }
    }

    public boolean addListenerOnly(IDeferredListener ls) {
        while (true) {
            switch (state) {
                case STATE_INIT:
                    if (this.pushListener1(ls))
                        return true;
                    continue;
                case STATE_SUCC:
                    return false;
                case STATE_ERRR:
                    return false;
                case STATE_LSTN:
                    if (this.pushListenerX(ls))
                        return true;
                    continue;
                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;
            }
        }
    }

    public boolean addListenerOnly(IFn onVal, IFn onErr) {
        return addListenerOnly(AListener.fromFn(onVal, onErr));
    }

    public Object addListener(Object lss) {
        IDeferredListener ls = (IDeferredListener) lss;
        while (true) {
            switch (state) {
                case STATE_INIT:
                    if (this.pushListener1(ls))
                        return true;
                    continue;
                case STATE_SUCC:
                    ls.onSuccess(value);
                    return false;
                case STATE_ERRR:
                    ls.onError(value);
                    return false;
                case STATE_LSTN:
                    if (this.pushListenerX(ls))
                        return true;
                    continue;
                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;
            }
        }
    }

    public Object onRealized(Object onSucc, Object onErr) {
        return this.addListener(AListener.fromFn(onSucc, onErr));
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
            byte state = this.state;
            switch (state) {
                case STATE_INIT:
                case STATE_LSTN: {
                    byte nst = (byte) STATE.compareAndExchange(this, state, STATE_LOCK);
                    if (nst != state) {
                        state = nst;
                        continue;
                    }
                    if (this.token == null) {
                        this.token = token;
                        this.state = state;
                        return true;
                    } else {
                        this.state = state;
                        return false;
                    }
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
        return this.state == STATE_ERRR ? value : fallback;
    }

    public Object unwrap() {
        return this.state == STATE_SUCC ? value : this;
    }

    public Object get() {
        switch (this.state) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                throw Util.sneakyThrow((Throwable) value);
            default:
                throw new IllegalStateException("kdeferred is not realized");
        }
    }

    private CountDownLatch acquireCountdDownLatch() {
        CountDownLatch cdl = new CountDownLatch(1);
        this.addListener(new CdlListener(cdl));
        return cdl;
    }

    public Object deref(long ms, Object timeoutValue) {

        switch (this.state) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                throw Util.sneakyThrow((Throwable) value);
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
                throw Util.sneakyThrow((Throwable) value);
        }

        return timeoutValue;
    }

    public Object deref() {

        switch (this.state) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                throw Util.sneakyThrow((Throwable) value);
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
                throw Util.sneakyThrow((Throwable) value);
        }

        throw new IllegalStateException();
    }

    public void revokeTo(KDeferred d) {
        if (d.revokable) {
            this.addListener(new KdRevokeListener(d));
        }
    }

    public void chain0(IDeferred x, Object token) {
        Object x1;
        while ((x1 = x.successValue(x)) != x) {
             if (x1 instanceof IDeferred) {
                x = (IDeferred) x1;
             } else {
                this.success0(this.state, x1, token);
                return;
             }
        }
        ChainListener ls = new ChainListener(this, token);
        if (x instanceof KDeferred) {
            KDeferred xx = (KDeferred) x;
            xx.addListener(ls);
            if (xx.revokable) {
                this.addListener(new KdRevokeListener(xx));
            }
        } else if (x instanceof IMutableDeferred) {
            ((IMutableDeferred) x).addListener(ls);
        } else {
            x.onRealized(ls.successCallback(), ls.errorCallback());
        }
    }

    public KDeferred chain(Object x, Object token) {
        if (x instanceof IDeferred) {
            this.chain0((IDeferred) x, token);
        } else {
            this.success0(this.state, x, token);
        }
        return this;
    }

    public KDeferred bind(AFn valFn, AFn errFn, Object token) {
        while (true) {
            switch (state) {
                case STATE_INIT: {
                    KDeferred dest = new KDeferred(token);
                    if (this.pushListener1(new BindListener(dest, valFn, errFn, token))) {
                        return dest;
                    } else {
                        continue;
                    }
                }
                case STATE_SUCC:
                    return wrap(valFn.invoke(value));
                case STATE_ERRR:
                    if (errFn == null) {
                        return this;
                    } else {
                        return wrap(errFn.invoke(value));
                    }
                case STATE_LSTN: {
                    KDeferred dest = new KDeferred(token);
                    if (this.pushListenerX(new BindListener(dest, valFn, errFn, token))) {
                        return dest;
                    } else {
                        continue;
                    }
                }
                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;
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
        return new KDeferred(STATE_ERRR, e);
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
            kd.pushListener1(new RevokeListener(d, canceller, errCallback));
            kd.chain(d, null);
            return kd;
        }
    }
}