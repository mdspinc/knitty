package knitty.javaimpl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;

import clojure.lang.Associative;
import clojure.lang.IEditableCollection;
import clojure.lang.ITransientAssociative;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.IExceptionInfo;
import clojure.lang.ExceptionInfo;
import clojure.lang.IPersistentMap;
import clojure.lang.ILookup;

import manifold.deferred.IDeferredListener;
import manifold.deferred.IMutableDeferred;

public final class YankCtx implements ILookup {

    private static final Object NONE = new Object();
    private static final int NOTRANSIENT_SIZE = 2;
    private static final int ASHIFT = 4;
    private static final int ASIZE = 1 << ASHIFT;
    private static final int AMASK = ASIZE - 1;

    private static final VarHandle AR0 = MethodHandles.arrayElementVarHandle(KDeferred[][].class);
    private static final VarHandle AR1 = MethodHandles.arrayElementVarHandle(KDeferred[].class);
    private static final VarHandle YSC = MethodHandles.arrayElementVarHandle(Yarn[].class);
    private static final VarHandle ADDED;
    private static final VarHandle FROZEN;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            ADDED = l.findVarHandle(YankCtx.class, "added", KVCons.class);
            FROZEN = l.findVarHandle(YankCtx.class, "frozen", Boolean.TYPE);
        } catch (ReflectiveOperationException var1) {
            throw new ExceptionInInitializerError(var1);
        }
    }

    final Associative inputs;
    private volatile KVCons added;
    private volatile boolean frozen;
    private final KDeferred[][] a0;
    private final KwMapper kwm;
    private final Yarn[] yarnsCache;
    private final YarnProvider yankerProvider;

    public final Object tracer;
    public final Object token;

    private static class KVCons {

        public final KVCons next;
        public final Keyword k;
        public final KDeferred d;

        public KVCons(KVCons next, Keyword k, KDeferred d) {
            this.next = next;
            this.k = k;
            this.d = d;
        }
    }

    public YankCtx(Associative inputs, YarnProvider yp, Object tracer) {
        this.kwm = KwMapper.getIntance();
        this.inputs = inputs;
        this.yankerProvider = yp;
        this.yarnsCache = yp.ycache();
        this.tracer = tracer;
        this.token = new Object();
        this.a0 = new KDeferred[((this.kwm.maxi() + AMASK) >> ASHIFT)][];
    }

    private KDeferred[] createChunk(int i0) {
        KDeferred[] a11 = new KDeferred[ASIZE];
        KDeferred[] a1x = (KDeferred[]) AR0.compareAndExchange(a0, i0, null, a11);
        return a1x == null ? a11 : a1x;
    }

    public void yank(Iterable<?> yarns, IDeferredListener callback) {

        int di = 0;
        KDeferred[] ds = null;

        for (Object x : yarns) {
            KDeferred r;
            if (x instanceof Keyword) {
                int i0 = this.kwm.getr((Keyword) x, true);
                if (i0 == -1) {
                    callback.onError(new IllegalArgumentException("unknown yarn " + x));
                    return;
                }
                r = this.fetch(i0);
            } else {
                Yarn y = (Yarn) x;
                int i0 = this.kwm.getr(y.key(), true);
                if (i0 == -1) {
                    callback.onError(new IllegalArgumentException("unknown yarn " + y.key()));
                    return;
                }
                r = this.fetch(i0, y);
            }
            if (r.state != KDeferred.STATE_SUCC) {
                if (di == 0) {
                    ds = new KDeferred[8];
                } if (di == ds.length) {
                    ds = Arrays.copyOf(ds, ds.length * 2);
                }
                ds[di++] = r;
            }
        }
        switch (di) {
            case 0:  callback.onSuccess(null); return;
            case 1:  ds[0].addListener(callback); return;
            default: KAwaiter.awaitArr(callback, ds, di); return;
        }
    }

    public Object valAt(Object key) {
        return this.valAt(key, null);
    }

    public Object valAt(Object key, Object fallback) {
        if ((key instanceof Keyword)) {
            Keyword k = (Keyword) key;
            if (k.getNamespace() != null) {
                int i = kwm.getr(k, false);
                if (i != -1) {
                    int i0 = i >> ASHIFT;
                    int i1 = i & AMASK;
                    KDeferred[] a1 = (KDeferred[]) AR0.getVolatile(a0, i0);
                    if (a1 != null) {
                        KDeferred v = (KDeferred) AR1.getVolatile(a1, i1);
                        if (v != null) {
                            return v.unwrap();
                        }
                    }
                }
            }
        }
        return inputs.valAt(key, fallback);
    }

    public KDeferred pull(int i) {
        int i0 = i >> ASHIFT;
        int i1 = i & AMASK;

        KDeferred[] a1 = (KDeferred[]) AR0.getVolatile(a0, i0);
        if (a1 == null) {
            a1 = this.createChunk(i0);
        }
        KDeferred v = (KDeferred) AR1.getVolatile(a1, i1);
        if (v != null) {
            return v;
        }

        KDeferred d = new KDeferred(token);
        KDeferred d0 = (KDeferred) AR1.compareAndExchange(a1, i1, null, d);
        return d0 != null ? d0 : d;
    }

    public KDeferred fetch(int i) {

        KDeferred d = pull(i);
        if (d.owned || d.owned()) {
            return d;
        }

        Keyword k = this.kwm.get(i);
        Object x = inputs.valAt(k, NONE);
        if (x != NONE) {
            return d.chainFrom(x, token);
        }

        Yarn y = this.yarn(i);
        y.yank(this, d);

        KVCons a = added;
        while (!ADDED.compareAndSet(this, a, new KVCons(a, k, d))) a = added;
        return d;
    }

    public KDeferred fetch(int i, Yarn y) {

        KDeferred d = pull(i);
        if (d.owned || d.owned()) {
            return d;
        }

        Keyword k = y.key();
        Object x = inputs.valAt(k, NONE);
        if (x != NONE) {
            return d.chainFrom(x, token);
        }

        y.yank(this, d);

        KVCons a = added;
        while (!ADDED.compareAndSet(this, a, new KVCons(a, k, d))) a = added;
        return d;
    }

    private Yarn yarn(int i) {
        Yarn y = (Yarn) YSC.getVolatile(yarnsCache, i);
        if (y != null) {
            return y;
        }
        y = yankerProvider.yarn(kwm.get(i));
        YSC.setVolatile(yarnsCache, i, y);
        return y;
    }

    public Associative freeze() {
        if ((boolean) FROZEN.getAndSet(this, true)) {
            throw new IllegalStateException("yankctx is already frozen");
        }
        KVCons added = this.added;
        if (added == null) {
            return inputs;
        }

        int c = 0;
        for (KVCons a = added; c < NOTRANSIENT_SIZE && a != null; a = a.next)
            c++;

        if (c == NOTRANSIENT_SIZE && inputs instanceof IEditableCollection) {
            ITransientAssociative t = (ITransientAssociative) ((IEditableCollection) inputs).asTransient();
            for (KVCons a = added; a != null; a = a.next) {
                t = t.assoc(a.k, a.d.unwrap());
            }
            return (Associative) t.persistent();
        } else {
            Associative t = inputs;
            for (KVCons a = added; a != null; a = a.next) {
                t = t.assoc(a.k, a.d.unwrap());
            }
            return t;
        }
    }

    public IDeferredListener canceller() {
        return new IDeferredListener() {
            public Object onSuccess(Object _v) {
                if (!frozen) cancel(null);
                return null;
            }
            public Object onError(Object e) {
                if (!frozen) cancel((Throwable) e);
                return null;
            }
        };
    }

    public void cancel(Throwable cause) {
        if ((boolean) FROZEN.getAndSet(this, true)) {
            throw new IllegalStateException("yankctx is already frozen");
        }
        CancellationException ex = new CancellationException("yankctx is cancelled");
        if (cause != null) {
            ex.addSuppressed(ex);
        }
        KVCons added = this.added;
        for (KVCons a = added; a != null; a = a.next) {
            a.d.error(ex, token);
        }
    }
}
