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

import manifold.deferred.IDeferredListener;

public final class MDM {

    private static Object KLOCK = new Object();
    private static Keyword[] KSA = new Keyword[1024];
    private static Map<Keyword, Long> KSM = new ConcurrentHashMap<>(1024);
    private static int KID;

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

    public static int maxid() {
        synchronized (KLOCK) {
            return KID;
        }
    }

    public static long regkw(Keyword k) {
        Long v = KSM.get(k);
        if (v != null) {
            return v.longValue();
        } else {
            synchronized (KLOCK) {
                long res = ++KID;
                KSM.put(k, res);
                if (res >= KSA.length) {
                    KSA = Arrays.copyOf(KSA, KSA.length * 2);
                }
                KSA[(int) res] = k;
                return res;
            }
        }
    }

    public static void resetKeywordsPoolForTests() {
        synchronized (KLOCK) {
            KSM.clear();
            KID = 0;
            KSA = new Keyword[1024];
        }
    }

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
    private final Keyword[] ksa;
    private final Yarn[] yarnsCache;
    private final YarnProvider yankerProvider;
    private final int kid;

    public final Object tracer;
    public final Object token;

    private static final class YankFreezer implements IDeferredListener {

        private static final Keyword YANKED_POY = Keyword.intern("knitty", "yanked-poy");
        private static final Keyword FAILED_POY = Keyword.intern("knitty", "failed-poy");
        private static final Keyword YANKED_YARNS = Keyword.intern("knitty", "yanked-yarns");

        private final MDM mdm;
        private final KDeferred result;
        private final Object yarns;

		private YankFreezer(MDM mdm, KDeferred result, Object yarns) {
			this.mdm = mdm;
            this.result = result;
            this.yarns = yarns;
		}

		public Object onSuccess(Object v) {
		    result.success(mdm.freeze(), mdm.token);
		    return null;
		}

		public Object onError(Object e) {
            Associative fpoy = mdm.freeze();
		    IPersistentMap exdata = (e instanceof IExceptionInfo) ? ((IExceptionInfo) e).getData() : null;

            if (exdata == null) {
                exdata = new PersistentArrayMap(
                    new Object[] {
                        YANKED_YARNS, yarns,
                        YANKED_POY, mdm.inputs,
                        FAILED_POY, fpoy,
                    });
            } else {
		        exdata = exdata
                    .assoc(YANKED_YARNS, yarns)
		            .assoc(YANKED_POY, mdm.inputs)
                    .assoc(FAILED_POY, fpoy);
            }
		    Throwable ex = new ExceptionInfo("failed to yank", exdata, (Throwable) e);
		    result.error(ex, mdm.token);
		    return null;
		}
	}

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
        this.inputs = inputs;
        this.ksa = KSA;
        this.yankerProvider = yp;
        this.yarnsCache = yp.ycache();
        this.tracer = tracer;
        this.token = new Object();
        this.kid = maxid();
        this.a0 = new KDeferred[((this.kid + AMASK) >> ASHIFT)][];
    }

    private KDeferred[] createChunk(int i0) {
        KDeferred[] a11 = new KDeferred[ASIZE];
        KDeferred[] a1x = (KDeferred[]) AR0.compareAndExchange(a0, i0, null, a11);
        return a1x == null ? a11 : a1x;
    }

    public static KDeferred yank0(Associative inputs, YarnProvider yp, Iterable<?> yarns, Object tracer) {

        MDM mdm = new MDM(inputs, yp, tracer);
        ArrayList<KDeferred> ds = new ArrayList<>();

        for (Object x : yarns) {

            KDeferred r;
            if (x instanceof Keyword) {
                Long i0 = KSM.get((Keyword) x);
                if (i0 == null) {
                    throw new IllegalArgumentException("unknown yarn " + x);
                }
                r = mdm.fetch(i0.longValue());
            } else {
                Yarn y = (Yarn) x;
                Long i0 = KSM.get(y.key());
                r = mdm.fetch(i0.longValue(), y);
            }

            if (r.state != KDeferred.STATE_SUCC) {
                ds.add(r);
            }
        }

        if (ds.isEmpty()) {
            return KDeferred.wrap(mdm.freeze());
        } else {
            KDeferred result = new KDeferred(mdm.token);
            KAwaiter.awaitIter(new YankFreezer(mdm, result, yarns), ds.iterator());
            return KDeferred.revoke(result, mdm::cancel);
        }
    }

    public KDeferred fetch(long il) {

        int i = (int) il;
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

        if (!fetchInput(i, d, this.ksa[i])) {
            Yarn y = this.yarn(i);
            y.yank(this, d);
        }

        return d;
    }

    public KDeferred fetch(long il, Yarn y) {

        int i = (int) il;
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
        y = yankerProvider.yarn(ksa[i]);
        YSC.setVolatile(yarnsCache, i, y);
        return y;
    }

    Associative freeze() {
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

    void cancel() {
        for (KVCons a = added; a != null; a = a.next) {
            a.d.error(CANCEL_EX, token);
        }
    }
}
