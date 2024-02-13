package knitty.javaimpl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

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

    private static class ChainFnListener implements IDeferredListener {

        private final KDeferred d;
        private final IFn vf;
        private final IFn ef;
        private final Object token;

        private ChainFnListener(KDeferred d, IFn vf, IFn ef, Object token) {
            this.d = d;
            this.vf = vf;
            this.ef = ef;
            this.token = token;
        }

        public Object onSuccess(Object v) {
            Object x;
            try {
                x = vf.invoke(v);
            } catch (Throwable e) {
                d.error(e, token);
                return null;
            }
            d.success(x, token);
            return null;
        }

        public Object onError(Object e) {
            d.error(ef.invoke(e), token);
            return null;
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

    private final static class ChainSucFn extends AFn {

        private final KDeferred kd;
        private final Object token;

        public ChainSucFn(KDeferred kd, Object token) {
            this.kd = kd;
            this.token = token;
        }

        public Object invoke(Object x) {
            if (x instanceof IDeferred) {
                kd.chainFrom(x, token);
            } else {
                kd.success(x, token);
            }
            return null;
        }
    }

    private final static class ChainErrFn extends AFn {

        private final KDeferred kd;
        private final Object token;

        public ChainErrFn(KDeferred kd, Object token) {
            this.kd = kd;
            this.token = token;
        }

        public Object invoke(Object x) {
            if (x instanceof IDeferred) {
                kd.chainFromDeferred((IDeferred) x, token);
            } else {
                kd.error(x, token);
            }
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
                kd.chainFromDeferred((IDeferred) x, token);
            } else {
                kd.success(x, token);
            }
            return null;
        }

        public Object onError(Object x) {
            if (x instanceof IDeferred) {
                kd.chainFromDeferred((IDeferred) x, token);
            } else {
                kd.error(x, token);
            }
            return null;
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

        private static final int MAX_SIZE = 32;

        public final IDeferredListener[] items;
        public int pos;
        public Listeners next;

        public Listeners(IDeferredListener x) {
            this.items = new IDeferredListener[] { x, null, null, null };
            this.pos = 1;
        }

        public Listeners(int size, IDeferredListener x) {
            this.items = new IDeferredListener[size];
            this.items[0] = x;
            this.pos = 1;
        }

        public Listeners push(IDeferredListener ls) {
            if (this.pos == this.items.length) {
                this.next = new Listeners(Math.min(MAX_SIZE, this.items.length * 2), ls);
                return next;
            } else {
                this.items[pos++] = ls;
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
    private static final VarHandle TOKEN;
    private static final VarHandle OWNED;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            STATE = l.findVarHandle(KDeferred.class, "state", Byte.TYPE);
            TOKEN = l.findVarHandle(KDeferred.class, "token", Object.class);
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
    volatile boolean owned;
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

    KDeferred(Object token, boolean revokable) {
        this.token = token;
        this.revokable = revokable;
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
                e0.printStackTrace();;
            }
        } catch (Throwable e1) {
            e0.printStackTrace();
            e1.printStackTrace();
        }
    }

    public Object success(Object x) {
        byte state = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK);
        if (state == STATE_INIT) {
            if (this.token != null) {
                this.state = STATE_INIT;
                throw new IllegalStateException("invalid claim-token");
            }
            this.value = x;
            this.state = STATE_SUCC;
            return true;
        }
        return success0(x, null);
    }

    public Object success(Object x, Object token) {
        byte state = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK);
        if (state == STATE_INIT) {
            if (this.token != token) {
                this.state = STATE_INIT;
                throw new IllegalStateException("invalid claim-token");
            }
            this.value = x;
            this.state = STATE_SUCC;
            return true;
        }
        return success0(x, token);
    }

    private Object success0(Object x, Object token) {
        while (true) {
            switch (this.state) {

                case STATE_INIT: {
                    byte state = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK);
                    if (state == STATE_INIT) break;

                    if (token != this.token) {
                        STATE.setVolatile(this, STATE_INIT);
                        throw new IllegalStateException("invalid claim-token");
                    }

                    this.value = x;
                    STATE.setVolatile(this, STATE_SUCC);
                    return true;
                }

                case STATE_LSTN: {

                    byte state = (byte) STATE.compareAndExchange(this, STATE_LSTN, STATE_LOCK);
                    if (state != STATE_LSTN) break;

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
                        for (int i = 0; i < node.pos; ++i) {
                            IDeferredListener ls = node.items[i];
                            try {
                                ls.onSuccess(value);
                            } catch (Throwable e) {
                                logException(e);
                            }
                        }
                    }

                    return true;
                }

                case STATE_SUCC:
                case STATE_ERRR:
                    return false;
            }
            Thread.onSpinWait();
        }
    }

    public Object error(Object x) {
        byte state = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK);
        if (state == STATE_INIT) {
            if (this.token != null) {
                this.state = STATE_INIT;
                throw new IllegalStateException("invalid claim-token");
            }
            this.value = x;
            this.state = STATE_ERRR;
            return true;
        }
        return this.error0(x, null);
    }

    public Object error(Object x, Object token) {
        byte state = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK);
        if (state == STATE_INIT) {
            if (this.token != token) {
                this.state = STATE_INIT;
                throw new IllegalStateException("invalid claim-token");
            }
            this.value = x;
            this.state = STATE_ERRR;
            return true;
        }
        return this.error0(x, token);
    }

    private Object error0(Object x, Object token) {
        while (true) {
            switch (this.state) {

                case STATE_INIT: {
                    byte state = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK);
                    if (state != STATE_INIT) break;

                    if (token != this.token) {
                        STATE.setVolatile(this, STATE_INIT);
                        throw new IllegalStateException("invalid claim-token");
                    }

                    this.value = x;
                    STATE.setVolatile(this, STATE_ERRR);
                    return true;
                }

                case STATE_LSTN: {

                    byte state = (byte) STATE.compareAndExchange(this, STATE_LSTN, STATE_LOCK);
                    if (state != STATE_LSTN) break;

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
                        for (int i = 0; i < node.pos; ++i) {
                            IDeferredListener ls = node.items[i];
                            try {
                                ls.onError(value);
                            } catch (Throwable e) {
                                logException(e);
                            }
                        }
                    }
                    return true;
                }

                case STATE_SUCC:
                case STATE_ERRR:
                    return false;
            }
            Thread.onSpinWait();
        }
    }

    private boolean pushListener1(IDeferredListener x) {

        byte state;
        while ((state = (byte) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK)) == STATE_LOCK)
            Thread.onSpinWait();

        if (state == STATE_INIT) {
            this.lcFirst = this.lcLast = new Listeners(x);
            STATE.setVolatile(this, STATE_LSTN);
            return true;
        } else {
            return false;
        }
    }

    private boolean pushListener(IDeferredListener x) {

        byte state;
        while ((state = (byte) STATE.compareAndExchange(this, STATE_LSTN, STATE_LOCK)) == STATE_LOCK)
            Thread.onSpinWait();

        if (state == STATE_LSTN) {
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
                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;
                case STATE_INIT:
                    if (this.pushListener1(ls))
                        return true;
                    else
                        continue;
                case STATE_LSTN:
                    if (this.pushListener(ls))
                        return true;
                    else
                        continue;
                case STATE_SUCC:
                    ls.onSuccess(value);
                    return false;
                case STATE_ERRR:
                    ls.onError(value);
                    return false;
                default:
                    throw new IllegalStateException();
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

    private CountDownLatch acquireCountdDownLatch() {
        CountDownLatch cdl = new CountDownLatch(1);
        this.addListener(new CdlListener(cdl));
        return cdl;
    }

    public KDeferred chainFrom(Object x) {
        return chainFrom(x, null);
    }

    void chainFromDeferred(IDeferred x, Object token) {
        if (x instanceof KDeferred) {
            KDeferred xx = (KDeferred) x;
            xx.addListener(new ChainListener(this, token));
            if (xx.revokable) {
                this.addListener(new KdRevokeListener(xx));
            }
        } else if (x instanceof IMutableDeferred) {
            ((IMutableDeferred) x).addListener(new ChainListener(this, token));
        } else {
            x.onRealized(new ChainSucFn(this, token), new ChainErrFn(this, token));
        }
    }

    public KDeferred chainFrom(Object x, Object token) {
        if (x instanceof IDeferred) {
            this.chainFromDeferred((IDeferred) x, token);
        } else {
            this.success(x, token);
        }
        return this;
    }

    public KDeferred chain(IFn vf, IFn ef) {
        KDeferred d = new KDeferred(this.token);
        d.pushListener1(new KdRevokeListener(this));
        this.addListener(new ChainFnListener(d, vf, ef, token));
        return d;
    }

    public static KDeferred create() {
        return new KDeferred();
    }

    public static KDeferred create(Object token) {
        return new KDeferred(token);
    }

    public static KDeferred wrapErr(Throwable e) {
        KDeferred d = new KDeferred();
        d.value = e;
        STATE.setVolatile(d, STATE_ERRR);
        return d;
    }

    public static KDeferred wrapDeferred(IDeferred x) {
        if (x instanceof KDeferred) {
             return (KDeferred) x;
        } else if (x instanceof IMutableDeferred) {
            KDeferred d = new KDeferred(x);
            IMutableDeferred xx = (IMutableDeferred) x;
            xx.addListener(new ChainListener(d, x));
            return d;
        } else {
            KDeferred d = new KDeferred(x);
            IDeferred xx = (IDeferred) x;
            xx.onRealized(new ChainSucFn(d, x), new ChainErrFn(d, x));
            return d;
        }
    }

    public static KDeferred wrap(Object x) {
        if (x instanceof IDeferred) {
            return wrapDeferred((IDeferred) x);
        } else {
            KDeferred d = new KDeferred();
            d.value = x;
            STATE.setVolatile(d, STATE_SUCC);
            return d;
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