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

    private final class BindListener implements IDeferredListener {

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

        public Object onSuccess(Object x) {
            Object t;
            try {
                t = valFn.invoke(x);
            } catch (Throwable e) {
                dest.error(e, token);
                return null;
            }
            if (t instanceof IDeferred) {
                dest.chainFrom(((IDeferred) t).successValue(t), token);
            } else {
                dest.success(t, token);
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
                    dest.error(e1, token);
                    return null;
                }
                if (t instanceof IDeferred) {
                    dest.chainFrom(((IDeferred) t).successValue(t), token);
                } else {
                    dest.success(t, token);
                }
                return null;
            }
        }
    }

    private static class FnListener implements IDeferredListener {
        private final IFn onSucc;
        private final IFn onErr;

        FnListener(IFn onSucc, IFn onErr) {
            this.onSucc = onSucc;
            this.onErr = onErr;
        }

        public Object onSuccess(Object x) {
            this.onSucc.invoke(x);
            return null;
        }

        public Object onError(Object e) {
            this.onErr.invoke(e);
            return null;
        }
    }

    private static class CdlListener implements IDeferredListener {
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

    private final static class ChainListener implements IDeferredListener {

        private final KDeferred kd;
        private final Object token;

        public ChainListener(KDeferred kd, Object token) {
            this.kd = kd;
            this.token = token;
        }

        public Object onSuccess(Object x) {
            if (x instanceof IDeferred) {
                kd.chainFrom(((IDeferred) x).successValue(x), token);
            } else {
                kd.success(x, token);
            }
            return null;
        }

        public Object onError(Object x) {
            if (x instanceof IDeferred) {
                kd.chainFrom(((IDeferred) x).successValue(x), token);
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

    private final static class RevokeListener implements IDeferredListener {

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

    private final static class KdRevokeListener implements IDeferredListener {

        private final KDeferred d;

        public KdRevokeListener(KDeferred d) {
            this.d = d;
        }

        public Object onSuccess(Object x) {
            if (!d.realized()) {
                d.error(RevokeException.INSTANCE, d.token);
            }
            return null;
        }

        public Object onError(Object x) {
            if (!d.realized()) {
                d.error(new RevokeException((Throwable) x), d.token);
            }
            return null;
        }
    }

    private static final class Listeners {

        private IDeferredListener l0;
        private IDeferredListener l1;
        private IDeferredListener l2;
        private IDeferredListener l3;

        public int pos;
        public Listeners next;

        public Listeners(IDeferredListener x) {
            this.l0 = x;
        }

        public IDeferredListener get(int i) {
            switch (i) {
                case 0:
                    return this.l0;
                case 1:
                    return this.l1;
                case 2:
                    return this.l2;
                case 3:
                    return this.l3;
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
                        this.l1 = ls;
                        break;
                    case 2:
                        this.l2 = ls;
                        break;
                    case 3:
                        this.l3 = ls;
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
        return owned || (boolean) OWNED.getAndSet(this, true);
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
            }
        } catch (Throwable e1) {
            e1.printStackTrace();
        }
    }

    public Object success(Object x) {
        return this.success(x, null);
    }

    public Object success(Object x, Object token) {
        while (true) {
            switch (this.state) {

                case STATE_INIT:
                    if (STATE.compareAndSet(this, STATE_INIT, STATE_LOCK)) {
                        if (token != this.token) {
                            STATE.setVolatile(this, STATE_INIT);
                            throw new IllegalStateException("invalid claim-token");
                        }
                        this.value = x;
                        STATE.setVolatile(this, STATE_SUCC);
                        return true;
                    }
                    continue;

                case STATE_SUCC:
                    return false;

                case STATE_ERRR:
                    return false;

                case STATE_LSTN:
                    if (STATE.compareAndSet(this, STATE_LSTN, STATE_LOCK)) {
                        if (token != this.token) {
                            STATE.setVolatile(this, STATE_LSTN);
                            throw new IllegalStateException("invalid claim-token");
                        }
                        Listeners node = this.lcFirst;
                        this.value = x;
                        STATE.setVolatile(this, STATE_SUCC);

                        this.lcLast = null;
                        this.lcFirst = null;
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
                    }
                    continue;

                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;
            }
        }
    }

    public Object error(Object x) {
        return this.error(x, null);
    }

    public Object error(Object x, Object token) {
        while (true) {
            switch (this.state) {

                case STATE_INIT:
                    if (STATE.compareAndSet(this, STATE_INIT, STATE_LOCK)) {
                        if (token != this.token) {
                            STATE.setVolatile(this, STATE_INIT);
                            throw new IllegalStateException("invalid claim-token");
                        }
                        this.value = x;
                        STATE.setVolatile(this, STATE_ERRR);
                        return true;
                    }
                    continue;

                case STATE_SUCC:
                    return false;

                case STATE_ERRR:
                    return false;

                case STATE_LSTN:
                    if (STATE.compareAndSet(this, STATE_LSTN, STATE_LOCK)) {
                        if (token != this.token) {
                            STATE.setVolatile(this, STATE_LSTN);
                            throw new IllegalStateException("invalid claim-token");
                        }
                        Listeners node = this.lcFirst;
                        this.value = x;
                        STATE.setVolatile(this, STATE_ERRR);

                        this.lcLast = null;
                        this.lcFirst = null;
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
                    }
                    continue;

                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;
            }
        }
    }

    private boolean pushListener1(IDeferredListener x) {
        if (STATE.compareAndSet(this, STATE_INIT, STATE_LOCK)) {
            this.lcFirst = this.lcLast = new Listeners(x);
            this.state = STATE_LSTN;
            return true;
        } else {
            return false;
        }
    }

    private boolean pushListener(IDeferredListener x) {
        if (STATE.compareAndSet(this, STATE_LSTN, STATE_LOCK)) {
            this.lcLast = this.lcLast.push(x);
            STATE.setVolatile(this, STATE_LSTN);
            return true;
        } else {
            return false;
        }
    }

    public Object addListener(Object lss) {
        IDeferredListener ls = (IDeferredListener) lss;
        while (true) {
            switch (state) {
                case STATE_INIT:
                    if (this.pushListener1(ls))
                        return true;
                    else
                        continue;
                case STATE_SUCC:
                    ls.onSuccess(value);
                    return false;
                case STATE_ERRR:
                    ls.onError(value);
                    return false;
                case STATE_LSTN:
                    if (this.pushListener(ls))
                        return true;
                    else
                        continue;
                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;
            }
        }
    }

    public Object onRealized(Object onSucc, Object onErr) {
        return this.addListener(new FnListener((IFn) onSucc, (IFn) onErr));
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
                    if (!STATE.compareAndSet(this, state, STATE_LOCK)) {
                        continue;
                    }
                    boolean res = this.token == null;
                    if (res) {
                        this.token = token;
                    }
                    STATE.setVolatile(this, state);
                    return res;
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

    public void chainFrom(Object x, Object token) {
        if (!(x instanceof IDeferred)) {
            this.success(x, token);
        } else {
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
                ((IDeferred) x).onRealized(ls.successCallback(), ls.errorCallback());
            }
        }
    }

    public KDeferred bind(IFn valFn, IFn errFn, Object token) {
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
                case STATE_SUCC: {
                    Object ret;
                    try {
                        ret = valFn.invoke(value);
                    } catch (Throwable e) {
                        return wrapErr(e);
                    }
                    return wrap(ret);
                }
                case STATE_ERRR: {
                    Object ret;
                    try {
                        ret = errFn.invoke(value);
                    } catch (Throwable e) {
                        return wrapErr(e);
                    }
                    return wrap(ret);
                }
                case STATE_LSTN: {
                    KDeferred dest = new KDeferred(token);
                    if (this.pushListener(new BindListener(dest, valFn, errFn, token))) {
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
            Object xx = x.successValue(x);
            KDeferred d = new KDeferred(xx);
            d.chainFrom(xx, xx);
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
            kd.chainFrom(d, null);
            return kd;
        }
    }
}