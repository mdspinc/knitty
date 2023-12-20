package knitty.javaimpl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;

import clojure.lang.Associative;
import clojure.lang.IEditableCollection;
import clojure.lang.ITransientAssociative;
import clojure.lang.Keyword;
import manifold.deferred.IDeferred;
import manifold.deferred.IMutableDeferred;

public final class MDM {

    private static Object KLOCK = new Object();
    private static Keyword[] KSA = new Keyword[1024];
    private static int KID;
    private static Map<Keyword, Integer> KSM = new ConcurrentHashMap<>(1024);

    private static final CancellationException CANCEL_EX = createCancelEx("mdm is cancelled");
    private static final CancellationException FROZEN_EX = createCancelEx("mdm is frozen");

    private static CancellationException createCancelEx(String msg) {
        CancellationException e = new CancellationException(msg);
        e.setStackTrace(new StackTraceElement[0]);
        return e;
    }

    private static final Object NONE = new Object();
    private static final Object NIL = new Object();

    private static final int ASHIFT = 5;
    private static final int ASIZE = 1 << ASHIFT;
    private static final int AMASK = ASIZE - 1;

    public static boolean isNone(Object x) {
        return x == NONE;
    }

    public static int maxid() {
        synchronized (KLOCK) {
            return KID;
        }
    }

    public static int regkw(Keyword k) {
        Integer v = KSM.get(k);
        if (v != null) {
            return v;
        } else {
            synchronized (KLOCK) {
                int res = ++KID;
                KSM.put(k, res);
                if (res >= KSA.length) {
                    KSA = Arrays.copyOf(KSA, KSA.length * 2);
                }
                KSA[res] = k;
                return res;
            }
        }
    }

    public static void resetKeywordsPoolForTests() {
        synchronized (KLOCK) {
            KID = 0;
            KSA = new Keyword[1024];
            KSM = new ConcurrentHashMap<>(1024);
        }
    }

    private static final VarHandle AR0 = MethodHandles.arrayElementVarHandle(Object[][].class);
    private static final VarHandle AR1 = MethodHandles.arrayElementVarHandle(Object[].class);

    private final Associative init;
    private final KaList<Integer> added;
    private final Object[][] a0;
    private final Keyword[] ksa;

    public static class Result {
        public final Object value;
        public final boolean claimed;

        public Result(boolean claimed, Object value) {
            this.claimed = claimed;
            this.value = value == NIL ? null : value;
        }
    }

    public MDM(Associative init) {
        this.init = init;
        this.added = new KaList<>(16);
        int maxid;
        synchronized (KLOCK) {
            this.ksa = KSA;
            maxid = KID;
        }
        this.a0 = new Object[(maxid >> ASHIFT) + 1][];
    }

    private Object[] chunk(int k) {
        int z = k >> ASHIFT;
        Object[] a1 = (Object[]) AR0.getVolatile(a0, z);
        if (a1 == null) {
            a1 = new Object[ASIZE];
            Object[] a1x = (Object[]) AR0.compareAndExchange(a0, z, null, a1);
            return a1x == null ? a1 : a1x;
        } else {
            return a1;
        }
    }

    public Result fetch(int i) {

        Object[] a1 = chunk(i);
        int i1 = i & AMASK;

        Object v = AR1.getVolatile(a1, i1);
        if (v == null) {
            Object vv = init.valAt(ksa[i], NONE);
            if (vv != NONE) {
                AR1.setVolatile(a1, i1, vv == null ? NIL : vv);
                return new Result(false, vv);
            }
        } else if (v != NONE) {
            return new Result(false, v);
        }

        // new item, was prewarmed by calling mdmGet
        KaDeferred d = new KaDeferred();
        Object d1 = AR1.compareAndExchange(a1, i1, v, (Object) d);
        if (v == d1) {
            boolean ok = added.push(i);
            if (!ok) {
                d.error(FROZEN_EX);
                return new Result(false, d);
            } else {
                return new Result(true, d);
            }
        } else {
            return new Result(false, d1);
        }
    }

    public Object get(int i) {
        Object[] a1 = chunk(i);
        int i1 = i & AMASK;

        Object v = AR1.getVolatile(a1, i1);
        if (v != null) {
            return unwrap1(v);
        }

        Object vv = init.valAt(ksa[i], NONE);
        AR1.setVolatile(a1, i1, vv == null ? NIL : vv);
        return vv;
    }

    public void put(int i, Object vv) {
        Object[] a1 = chunk(i);
        int i1 = i & AMASK;
        AR1.setVolatile(a1, i1, vv == null ? NIL : vv);
    }

    private static Object unwrap1(Object x) {
        if (x instanceof IDeferred) {
            return ((IDeferred) x).successValue(x);
        } else if (x == NIL) {
            return null;
        } else {
            return x;
        }
    }

    public Associative freeze() {
        if (init instanceof IEditableCollection) {
            ITransientAssociative t = (ITransientAssociative) ((IEditableCollection) init).asTransient();
            for (int i : added) {
                Object x = unwrap1(AR1.getVolatile(chunk(i), i & AMASK));
                t = t.assoc(ksa[i], x);
            }
            return (Associative) t.persistent();
        } else {
            Associative t = init;
            for (int i : added) {
                Object x = unwrap1(AR1.getVolatile(chunk(i), i & AMASK));
                t = t.assoc(ksa[i], x);
            }
            return t;
        }
    }

    public void cancel() {
        for (int i : added) {
            Object x = AR1.getVolatile(chunk(i), i & AMASK);
            if (x instanceof IMutableDeferred) {
                ((IMutableDeferred) x).error(CANCEL_EX);
            }
        }
    }
}
