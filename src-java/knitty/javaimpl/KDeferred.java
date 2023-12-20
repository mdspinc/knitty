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
        IDeferredListener,
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

    private static final class SuccCallback extends AFn {
        private final IDeferredListener ls;

        public SuccCallback(IDeferredListener ls) {
            this.ls = ls;
        }

        public Object invoke(Object x) {
            return ls.onSuccess(x);
        }
    }

    private static final class FailCallback extends AFn {
        private final IDeferredListener ls;

        public FailCallback(IDeferredListener ls) {
            this.ls = ls;
        }

        public Object invoke(Object x) {
            return ls.onError(x);
        }
    }

    private static class Awaiter implements IDeferredListener {

        private final Object[] da;
        private final IDeferredListener ls;
        private int i;
        private IDeferred cd;

        public Awaiter(Object[] da, IDeferredListener ls) {
            this.da = da;
            this.ls = ls;
            this.i = da.length;
        }

        public Object onError(Object e) {
            return ls.onError(e);
        }

        public Object onSuccess(Object x) {
            Object d = this.cd;

            while (true) {

                while (d instanceof IDeferred) {
                    IDeferred dd = (IDeferred) d;
                    Object ndd = dd.successValue(dd);
                    if (dd == ndd) {
                        this.cd = dd;
                        if (dd instanceof IMutableDeferred) {
                            ((IMutableDeferred) d).addListener(this);
                        } else {
                            dd.onRealized(new SuccCallback(this), new FailCallback(this));
                        }
                        return null;
                    } else {
                        d = ndd;
                    }
                }

                if (i == 0)
                    break;
                d = this.da[--i];
            }

            try {
                ls.onSuccess(null);
            } catch (Throwable e) {
                logException(e);
            }
            return null;
        }
    }

    private static final class ListenersChunk {

        private static final int MAX_SIZE = 256;

        public final IDeferredListener[] items;
        public int pos;
        public ListenersChunk next;

        public ListenersChunk(IDeferredListener x) {
            this.items = new IDeferredListener[] {x, null, null, null, null};
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

    private static final int STATE_LOCK = -1;
    private static final int STATE_INIT = 0;
    private static final int STATE_LSTN = 1;
    private static final int STATE_SUCC = 2;
    private static final int STATE_ERRR = 3;

    private static final VarHandle STATE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            STATE = l.findVarHandle(KDeferred.class, "state", Integer.TYPE);
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
    private Object value; // sync by 'state'
    private ListenersChunk lcFirst;
    private ListenersChunk lcLast;
    private IMutableDeferred revokee;
    private IPersistentMap meta; // sychronized

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

    private static void logException(Throwable e) {
        try {
            LOG_EXCEPTION.invoke(e);
        } catch (Throwable e0) {
            e.printStackTrace();
        }
    }

    public KDeferred chainTo(Object x) {
        if (x instanceof IDeferred) {
            if (x instanceof IMutableDeferred) {
                ((IMutableDeferred) x).addListener(this);
            } else {
                ((IDeferred) x).onRealized(new SuccCallback(this), new FailCallback(this));
            }
        } else {
            this.success(x);
        }
        return this;
    }

    public Object success(Object x) {

        while (true) {
            switch (this.state) {

                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;

                case STATE_INIT: {

                    int state;
                    while ((state = (int) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK)) == STATE_LOCK)
                        Thread.onSpinWait();

                    if (state == STATE_INIT) {
                        this.value = x;
                        STATE.setVolatile(this, STATE_SUCC);
                        return null;
                    }
                }
                case STATE_LSTN: {

                    int state;
                    while ((state = (int) STATE.compareAndExchange(this, STATE_LSTN, STATE_LOCK)) == STATE_LOCK)
                        Thread.onSpinWait();

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

                        return null;
                    }
                }

                case STATE_SUCC:
                case STATE_ERRR:
                    return null;

                default:
                    throw new IllegalStateException();
            }
        }
    }

    public Object error(Object x) {

        while (true) {
            switch (this.state) {

                case STATE_LOCK:
                    Thread.onSpinWait();
                    continue;

                case STATE_INIT: {

                    int state;
                    while ((state = (int) STATE.compareAndExchange(this, STATE_INIT, STATE_LOCK)) == STATE_LOCK)
                        Thread.onSpinWait();

                    if (state == STATE_INIT) {
                        this.value = x;
                        STATE.setVolatile(this, STATE_ERRR);
                        return null;
                    }
                }

                case STATE_LSTN: {

                    int state;
                    while ((state = (int) STATE.compareAndExchange(this, STATE_LSTN, STATE_LOCK)) == STATE_LOCK)
                        Thread.onSpinWait();

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

                        return null;
                    }
                }

                case STATE_SUCC:
                case STATE_ERRR:
                    return null;

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
                        return null;
                    else
                        continue;
                case STATE_LSTN:
                    if (this.pushListener(ls))
                        return null;
                    else
                        continue;
                case STATE_SUCC:
                    return ls.onSuccess(value);
                case STATE_ERRR:
                    return ls.onError(value);
                default:
                    throw new IllegalStateException();
            }
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
    }

    public static KDeferred wrap(Object d) {
        if (d instanceof KDeferred) {
            return (KDeferred) d;
        }
        KDeferred kd = new KDeferred();
        kd.setRevokee(d);
        return kd.chainTo(d);
    }

    public Object onRealized(Object onSucc, Object onErr) {
        return this.addListener(new FnListener((IFn) onSucc, (IFn) onErr));
    }

    public Object cancelListener(Object listener) {
        throw new UnsupportedOperationException("cancelling of listeners is not supported");
    }

    public Object claim() {
        return null;
    }

    public Object error(Object x, Object token) {
        throw new UnsupportedOperationException("claiming is not supported");
    }

    public Object success(Object x, Object token) {
        throw new UnsupportedOperationException("claiming is not supported");
    }

    public Object onSuccess(Object x) {
        this.chainTo(x);
        return null;
    }

    public Object onError(Object e) {
        this.error(e);
        return null;
    }

    public Object executor() {
        return null;
    }

    public boolean realized() {
        switch (this.state) {
            case STATE_SUCC:
            case STATE_ERRR:
                return true;
            default:
                return false;
        }
    }

    public boolean isRealized() {
        return realized();
    }

    public Object successValue(Object fallback) {
        switch (this.state) {
            case STATE_SUCC:
                return value;
            default:
                return fallback;
        }
    }

    public Object errorValue(Object fallback) {
        switch (this.state) {
            case STATE_ERRR:
                return value;
            default:
                return fallback;
        }
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

    public static void awaitAll(IDeferredListener ls, Object... ds) {
        new Awaiter(ds, ls).onSuccess(null);
    }
}