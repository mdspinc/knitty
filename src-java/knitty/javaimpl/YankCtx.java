package knitty.javaimpl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import clojure.lang.Associative;
import clojure.lang.IEditableCollection;
import clojure.lang.ITransientAssociative;
import clojure.lang.Keyword;
import clojure.lang.ILookup;
import clojure.lang.IFn;
import manifold.deferred.IDeferredListener;

public final class YankCtx implements ILookup {

    private static final Object NONE = new Object();
    private static final KVCons NIL = new KVCons(null, null, null);
    private static final IFn KEYFN = Keyword.find("key");

    private static final int ASHIFT = 4;
    private static final int ASIZE = 1 << ASHIFT;
    private static final int AMASK = ASIZE - 1;

    private static final VarHandle AR0 = MethodHandles.arrayElementVarHandle(KDeferred[][].class);
    private static final VarHandle AR1 = MethodHandles.arrayElementVarHandle(KDeferred[].class);
    private static final VarHandle YSC = MethodHandles.arrayElementVarHandle(IFn[].class);
    private static final VarHandle ADDED;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            ADDED = l.findVarHandle(YankCtx.class, "added", KVCons.class);
        } catch (ReflectiveOperationException var1) {
            throw new ExceptionInInitializerError(var1);
        }
    }

    final Associative inputs;
    private volatile KVCons added;

    private final KDeferred[][] a0;
    private final KwMapper kwm;
    private final IFn[] yarnsCache;
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
        this.added = NIL;
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
                Keyword k = (Keyword) x;
                int i0 = this.kwm.getr(k, true);
                if (i0 == -1) {
                    callback.onError(new IllegalArgumentException("unknown yarn " + x));
                    return;
                }
                r = this.fetch(i0, k);
            } else {
                IFn y = (IFn) x;
                Keyword k = (Keyword) KEYFN.invoke(y.invoke());
                int i0 = this.kwm.getr(k, true);
                if (i0 == -1) {
                    callback.onError(new IllegalArgumentException("unknown yarn " + k));
                    return;
                }
                r = this.fetch(i0, k, y);
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

    public KDeferred yank1(Object x) {
        if (x instanceof Keyword) {
            Keyword k = (Keyword) x;
            int i0 = this.kwm.getr((Keyword) x, true);
            if (i0 == -1) {
                return KDeferred.wrapErr(new IllegalArgumentException("unknown yarn " + x));
            }
            return this.fetch(i0, k);
        } else {
            IFn y = (IFn) x;
            Keyword k = (Keyword) KEYFN.invoke(y.invoke());
            int i0 = this.kwm.getr(k, true);
            if (i0 == -1) {
                return KDeferred.wrapErr(new IllegalArgumentException("unknown yarn " + k));
            }
            return this.fetch(i0, k, y);
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

    public KDeferred fetch(int i, Keyword k) {

        KDeferred d = pull(i);
        if (d.owned()) {
            return d;
        }

        Object x = inputs.valAt(k, NONE);
        if (x != NONE) {
            d.chainFrom(x, token);
            return d;
        }

        IFn y = this.yarn(i);

        KVCons a = added;
        while (a != null && !ADDED.compareAndSet(this, a, new KVCons(a, k, d))) a = added;

        if (a == null) {
            d.error(YankFinishedException.INTANCE, token);

        } else {
            y.invoke(this, d);
        }

        return d;
    }

    public Object token() {
        return token;
    }

    public KDeferred fetch(int i, Keyword k, IFn y) {

        KDeferred d = pull(i);
        if (d.owned()) {
            return d;
        }

        Object x = inputs.valAt(k, NONE);
        if (x != NONE) {
            d.chainFrom(x, token);
            return d;
        }

        KVCons a = added;
        while (a != null && !ADDED.compareAndSet(this, a, new KVCons(a, k, d))) a = added;

        if (a == null) {
            d.error(YankFinishedException.INTANCE, token);
        } else {
            y.invoke(this, d);
        }

        return d;
    }

    private IFn yarn(int i) {
        IFn y = (IFn) YSC.getVolatile(yarnsCache, i);
        if (y != null) {
            return y;
        }
        y = yankerProvider.yarn(kwm.get(i));
        YSC.setVolatile(yarnsCache, i, y);
        return y;
    }

    private KVCons freeze() {
        KVCons a = (KVCons) ADDED.getAndSet(this, null);
        if (a == null) {
            throw new IllegalStateException("yankctx is already frozen");
        }
        return a;
    }

    public boolean isFrozen() {
        return added == null;
    }

    public void freezeVoid() {
        for (KVCons a = this.freeze(); a.d != null; a = a.next) {
            if (!a.d.owned()) {
                a.d.error(RevokeException.INSTANCE, this.token);
            }
        }
    }

    public Associative freezePoy() {
        KVCons added = this.freeze();
        if (added == NIL) {
            return inputs;
        }
        if (added.next.d != null && inputs instanceof IEditableCollection) {
            ITransientAssociative t = (ITransientAssociative) ((IEditableCollection) inputs).asTransient();
            for (KVCons a = added; a.d != null; a = a.next) {
                if (a.d.owned()) {
                    t = t.assoc(a.k, a.d.unwrap());
                } else {
                    a.d.error(RevokeException.INSTANCE, this.token);
                }
            }
            return (Associative) t.persistent();
        } else {
            Associative t = inputs;
            for (KVCons a = added; a.d != null; a = a.next) {
                if (a.d.owned()) {
                    t = t.assoc(a.k, a.d.unwrap());
                } else {
                    a.d.error(RevokeException.INSTANCE, this.token);
                }
            }
            return t;
        }
    }

    public IDeferredListener canceller() {
        return new IDeferredListener() {
            public Object onSuccess(Object _v) {
                if (!isFrozen()) {
                    cancel(null);
                }
                return null;
            }
            public Object onError(Object e) {
                if (!isFrozen()) {
                    Throwable t;
                    try {
                        t = (Throwable) e;
                    } catch (ClassCastException e1) {
                        t = e1;
                    }
                    cancel(t);
                }
                return null;
            }
        };
    }

    void cancel() {
        cancel(null);
    }

    public void cancel(Throwable cause) {
        CancellationException ex = new CancellationException("yankctx is cancelled");
        if (cause != null) {
            ex.initCause(ex);
        }
        for (KVCons a = this.freeze(); a.d != null; a = a.next) {
            a.d.error(ex, token);
        }
    }
}
