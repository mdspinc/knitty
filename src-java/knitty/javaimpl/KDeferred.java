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

    private static class FnListener implements IDeferredListener {
        private final IFn onSucc;
        private final IFn onErr;

        FnListener(IFn onSucc, IFn onErr) {
            this.onSucc = onSucc;
            this.onErr = onErr;
        }

        public Object onSuccess(Object x) {
            return this.onSucc.invoke(x);
        }

        public Object onError(Object e) {
            return this.onErr.invoke(e);
        }
    }

    private static class CountDownListener implements IDeferredListener {
        final CountDownLatch cdl;

        CountDownListener(CountDownLatch cdl) {
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

    private final static class ChainFnSucc extends AFn {

        private final KDeferred kd;
        private final Object token;

        public ChainFnSucc(KDeferred kd, Object token) {
            this.kd = kd;
            this.token = token;
        }

        public Object invoke(Object x) {
            if (x instanceof IDeferred) {
                kd.chainFrom(x, token);
            } else {
                kd.success(x, token);
            }
        }
    }

    private final static class ChainFnErrr extends AFn {

        private final KDeferred kd;
        private final Object token;

        public ChainFnErrr(KDeferred kd, Object token) {
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
        private final Runnable canceller;

        public RevokeListener(IDeferred d, Runnable canceller) {
            this.d = d;
            this.canceller = canceller;
        }

        public Object onSuccess(Object x) {
            if (!d.realized())
                canceller.run();
            return null;
        }

        public Object onError(Object x) {
            if (!d.realized())
                canceller.run();
            return null;
        }
    }

    private static final class ListenersChunk {

        private static final int MAX_SIZE = 32;

        public final IDeferredListener[] items;
        public int pos;
        public ListenersChunk next;

        public ListenersChunk(IDeferredListener x) {
            this.items = new IDeferredListener[] { x, null, null, null };
            this.pos = 1;
        }

        public ListenersChunk(int size, IDeferredListener x) {
            this.items = new IDeferredListener[size];
            this.items[0] = x;
            this.pos = 1;
        }

        public ListenersChunk push(IDeferredListener ls) {
            if (this.pos == this.items.length) {
                this.next = new ListenersChunk(Math.min(MAX_SIZE, this.items.length * 2), ls);
                return next;
            } else {
                this.items[pos++] = ls;
                return this;
            }
        }
    }

    private static final int STATE_INIT = 0;
    private static final int STATE_LOCK = 1;
    private static final int STATE_LSTN = 2;
    private static final int STATE_SUCC = 4;
    private static final int STATE_ERRR = 8;
    private static final int STATE_DONE_MASK = STATE_SUCC | STATE_ERRR;

    private static final VarHandle STATE;
    private static final VarHandle TOKEN;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            STATE = l.findVarHandle(KDeferred.class, "state", Integer.TYPE);
            TOKEN = l.findVarHandle(KDeferred.class, "token", Object.class);
        } catch (ReflectiveOperationException var1) {
            throw new ExceptionInInitializerError(var1);
        }
    }

    private static volatile IFn LOG_EXCEPTION = new AFn() {
        public Object invoke(Object ex) {
            ((Throwable) ex).printStackTrace();
            return null;
        }
    };

    public static void setExceptionLogFn(IFn f) {
        LOG_EXCEPTION = f;
    }

    private volatile int state;
    private volatile Object token;
    private Object value;
    private ListenersChunk lcFirst;
    private ListenersChunk lcLast;
    private IMutableDeferred revokee;
    private IPersistentMap meta;

    public KDeferred() {
        // do nothing
    }

    public KDeferred(Object token) {
        this.token = token;
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

    static void logException(Throwable e) {
        try {
            LOG_EXCEPTION.invoke(e);
        } catch (Throwable e0) {
            e.printStackTrace();
        }
    }

    public Object success(Object x) {
        return success(x, null);
    }

    public Object success(Object x, Object token) {
        while (true) {
            switch (this.state) {

                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;

                case STATE_INIT: {

                    int state;
                    while ((state = (int) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK)) == STATE_LOCK)
                        Thread.onSpinWait();

                    if (token != this.token) {
                        STATE.setVolatile(this, STATE_INIT);
                        throw new IllegalStateException("invalid claim-token");
                    }

                    if (state == STATE_INIT) {
                        this.value = x;
                        STATE.setVolatile(this, STATE_SUCC);
                        return Boolean.TRUE;
                    }
                }
                case STATE_LSTN: {

                    int state;
                    while ((state = (int) STATE.compareAndExchange(this, STATE_LSTN, STATE_LOCK)) == STATE_LOCK)
                        Thread.onSpinWait();

                    if (token != this.token) {
                        STATE.setVolatile(this, STATE_LSTN);
                        throw new IllegalStateException("invalid claim-token");
                    }

                    if (state == STATE_LSTN) {
                        ListenersChunk node = this.lcFirst;
                        this.value = x;
                        STATE.setVolatile(this, STATE_SUCC);

                        this.lcLast = null;
                        this.lcFirst = null;

                        IMutableDeferred r = this.revokee;
                        if (r != null) {
                            this.revokee = null;
                            try {
                                r.success(x);
                            } catch (Throwable e) {
                                logException(e);
                            }
                        }

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

                        return Boolean.TRUE;
                    }
                }

                case STATE_SUCC:
                case STATE_ERRR:
                    return Boolean.FALSE;

                default:
                    throw new IllegalStateException();
            }
        }
    }

    public Object error(Object x) {
        return this.error(x, null);
    }

    public Object error(Object x, Object token) {
        while (true) {
            switch (this.state) {

                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;

                case STATE_INIT: {

                    int state;
                    while ((state = (int) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK)) == STATE_LOCK)
                        Thread.onSpinWait();

                    if (token != this.token) {
                        STATE.setVolatile(this, STATE_INIT);
                        throw new IllegalStateException("invalid claim-token");
                    }

                    if (state == STATE_INIT) {
                        this.value = x;
                        STATE.setVolatile(this, STATE_ERRR);
                        return Boolean.TRUE;
                    }
                }

                case STATE_LSTN: {

                    int state;
                    while ((state = (int) STATE.compareAndExchange(this, STATE_LSTN, STATE_LOCK)) == STATE_LOCK)
                        Thread.onSpinWait();

                    if (token != this.token) {
                        STATE.setVolatile(this, STATE_LSTN);
                        throw new IllegalStateException("invalid claim-token");
                    }

                    if (state == STATE_LSTN) {
                        ListenersChunk node = this.lcFirst;
                        this.value = x;
                        STATE.setVolatile(this, STATE_ERRR);

                        this.lcLast = null;
                        this.lcFirst = null;

                        IMutableDeferred r = this.revokee;
                        if (r != null) {
                            this.revokee = null;
                            try {
                                r.error(x);
                            } catch (Throwable e) {
                                logException(e);
                            }
                        }

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

                        return Boolean.TRUE;
                    }
                }

                case STATE_SUCC:
                case STATE_ERRR:
                    return Boolean.FALSE;

                default:
                    throw new IllegalStateException();
            }
        }
    }

    private boolean pushListener1(IDeferredListener x) {

        int state;
        while ((state = (int) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK)) == STATE_LOCK)
            Thread.onSpinWait();

        if (state == STATE_INIT) {
            this.lcFirst = this.lcLast = new ListenersChunk(x);
            STATE.setVolatile(this, STATE_LSTN);
            return true;
        } else {
            return false;
        }
    }

    private boolean pushListener(IDeferredListener x) {

        int state;
        while ((state = (int) STATE.compareAndExchange(this, STATE_LSTN, STATE_LOCK)) == STATE_LOCK)
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
                        return Boolean.TRUE;
                    else
                        continue;
                case STATE_LSTN:
                    if (this.pushListener(ls))
                        return Boolean.TRUE;
                    else
                        continue;
                case STATE_SUCC:
                    ls.onSuccess(value);
                    return Boolean.FALSE;
                case STATE_ERRR:
                    ls.onError(value);
                    return Boolean.FALSE;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    boolean addAwaitListener(IDeferredListener ls) {
        switch (state) {
            case STATE_SUCC:
                return false;
            case STATE_ERRR:
                ls.onError(this.value);
                return true;
            default:
                this.addListener(ls);
                return true;
        }
    }

    public void setRevokee(Object x) {
        if (x instanceof IMutableDeferred) {
            IMutableDeferred r = (IMutableDeferred) x;
            this.revokee = r;
            while (true) {
                switch (state) {
                    case STATE_LOCK:
                        Thread.onSpinWait();
                        continue;
                    case STATE_INIT:
                        return;
                    case STATE_LSTN:
                        return;
                    case STATE_SUCC:
                        r.success(value);
                        return;
                    case STATE_ERRR:
                        r.error(value);
                        return;
                    default:
                        throw new IllegalStateException();
                }
            }
        }
        return;
    }

    public KDeferred chainFrom(Object x) {
        return chainFrom(x, null);
    }

    private void chainFromDeferred(IDeferred x, Object token) {
        if (x instanceof IMutableDeferred) {
            ((IMutableDeferred) x).addListener(new ChainListener(this, token));
        } else {
            ((IDeferred) x).onRealized(new ChainFnSucc(this, token), new ChainFnErrr(this, token));
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
        if (!TOKEN.compareAndSet(this, null, token)) {
            return false;
        }
        while (true) {
            switch (this.state) {
                case STATE_INIT:
                case STATE_LSTN:
                    return true;
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

    public Object deref(long ms, Object timeoutValue) {

        switch (this.state) {
            case STATE_SUCC:
                return value;
            case STATE_ERRR:
                throw Util.sneakyThrow((Throwable) value);
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
        this.addListener(new CountDownListener(cdl));
        return cdl;
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

    public static KDeferred wrap(Object x) {
        if (x instanceof IDeferred) {
            if (x instanceof KDeferred) {
                return (KDeferred) x;
            } else if (x instanceof IMutableDeferred) {
                KDeferred d = new KDeferred(x);
                IMutableDeferred xx = d.revokee = (IMutableDeferred) x;
                xx.addListener(new ChainListener(d, x));
                return d;
            } else {
                KDeferred d = new KDeferred(x);
                IDeferred xx = (IDeferred) x;
                xx.onRealized(new ChainFnSucc(d, x), new ChainFnErrr(d, x));
                return d;
            }
        } else {
            KDeferred d = new KDeferred();
            d.value = x;
            STATE.setVolatile(d, STATE_SUCC);
            return d;
        }
    }

    public static KDeferred revoke(IDeferred d, Runnable canceller) {
        if (d.realized()) {
            return wrap(d.successValue(d));
        } else {
            KDeferred kd = new KDeferred();
            kd.pushListener1(new RevokeListener(d, canceller));
            kd.chainFrom(d, null);
            return kd;
        }
    }
}