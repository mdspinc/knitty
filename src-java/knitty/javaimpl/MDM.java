package knitty.javaimpl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
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

public final class MDM implements ILookup {

    private static final CancellationException CANCEL_EX = createCancelEx("mdm is cancelled");
    private static CancellationException createCancelEx(String msg) {
        CancellationException e = new CancellationException(msg);
        e.setStackTrace(new StackTraceElement[0]);
        return e;
    }

    private static final Object NONE = new Object();

    private static final int ASHIFT = 5;
    private static final int ASIZE = 1 << ASHIFT;
    private static final int AMASK = ASIZE - 1;

    private static final VarHandle AR0 = MethodHandles.arrayElementVarHandle(KDeferred[][].class);
    private static final VarHandle AR1 = MethodHandles.arrayElementVarHandle(KDeferred[].class);
    private static final VarHandle YSC = MethodHandles.arrayElementVarHandle(Yarn[].class);
    private static final VarHandle ADDED;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            ADDED = l.findVarHandle(MDM.class, "added", KVCons.class);
        } catch (ReflectiveOperationException var1) {
            throw new ExceptionInInitializerError(var1);
        }
    }

    final Associative inputs;
    private volatile KVCons added;
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

    public MDM(Associative inputs, YarnProvider yp, Object tracer) {
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

    public KDeferred yank(Iterable<?> yarns) {
        ArrayList<KDeferred> ds = new ArrayList<>();

        for (Object x : yarns) {
            KDeferred r;
            if (x instanceof Keyword) {
                int i0 = this.kwm.getr((Keyword) x);
                r = this.fetch(i0);
            } else {
                Yarn y = (Yarn) x;
                int i0 = this.kwm.getr(y.key());
                r = this.fetch(i0, y);
            }
            if (r.state != KDeferred.STATE_SUCC) {
                ds.add(r);
            }
        }

        if (ds.isEmpty()) {
            return KDeferred.wrap(this);
        } else {
            KDeferred result = new KDeferred(this.token);
            KAwaiter.awaitIter(result.chainFromSupplierCallback(() -> this, token), ds.iterator());
            return result;
        }
    }

    public Object valAt(Object key) {
        return this.valAt(key, null);
    }

    public Object valAt(Object key, Object fallback) {
        if (!(key instanceof Keyword)) {
            return inputs.valAt(key, fallback);
        }

        Keyword k = (Keyword) key;

        int i = kwm.getr(k); // fixme
        int i0 = i >> ASHIFT;
        int i1 = i & AMASK;

        KDeferred[] a1 = (KDeferred[]) AR0.getVolatile(a0, i0);
        if (a1 == null) {
            return fallback;
        }
        KDeferred v = (KDeferred) AR1.getVolatile(a1, i1);
        return v == null ? inputs.valAt(key, fallback) : v.unwrap();
    }

    public KDeferred fetch(int i) {

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
        if (d0 != null) {
            return d0;
        }

        if (!fetchInput(i, d, this.kwm.get(i))) {
            Yarn y = this.yarn(i);
            y.yank(this, d);
        }

        return d;
    }

    public KDeferred fetch(int i, Yarn y) {

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
        if (d0 != null) {
            return d0;
        }

        if (!fetchInput(i, d, y.key())) {
            y.yank(this, d);
        }
        return d;
    }

    private boolean fetchInput(int i, KDeferred d, Keyword k) {
        Object x = inputs.valAt(k, NONE);
        if (x != NONE) {
            d.chainFrom(x, token);
            return true;
        }
        while (true) {
            KVCons a = added;
            if (ADDED.compareAndSet(this, a, new KVCons(a, k, d)))
                break;
        }
        return false;
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
        if (inputs instanceof IEditableCollection) {
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

    public void cancel() {
        for (KVCons a = added; a != null; a = a.next) {
            a.d.error(CANCEL_EX, token);
        }
    }
}
