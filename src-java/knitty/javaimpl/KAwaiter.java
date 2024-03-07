package knitty.javaimpl;

import java.util.Iterator;

import manifold.deferred.IDeferred;
import manifold.deferred.IDeferredListener;

public final class KAwaiter {

    private static byte OK = KDeferred.STATE_SUCC;

    public static void await(IDeferredListener ls) {
        ls.onSuccess(null);
    }

    public static void await(IDeferredListener ls, KDeferred x1) {
        if (x1.state != OK) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2) {
        if (x2.state != OK) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != OK) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3) {
        if (x3.state != OK) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != OK) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != OK) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4) {
        if (x4.state != OK) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != OK) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != OK) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != OK) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5) {
        if ((OK & x1.state & x2.state & x3.state & x4.state & x5.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4, x5);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6) {
        if ((OK & x1.state & x2.state & x3.state & x4.state & x5.state & x6.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4, x5, x6);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7) {
        if ((OK & x1.state & x2.state & x3.state & x4.state & x5.state & x6.state & x7.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4, x5, x6, x7);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8) {
        if ((OK & OK & x1.state & x2.state & x3.state & x4.state & x5.state & x6.state & x7.state & x8.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9) {
        if ((OK & x1.state & x2.state & x3.state & x4.state & x5.state & x6.state & x7.state & x8.state & x9.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10) {
        if ((OK & x1.state & x2.state & x3.state & x4.state & x5.state & x6.state & x7.state & x8.state & x9.state & x10.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11) {
        if ((OK & x1.state & x2.state & x3.state & x4.state & x5.state & x6.state & x7.state & x8.state & x9.state & x10.state & x11.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11, KDeferred x12) {
        if ((OK & x1.state & x2.state & x3.state & x4.state & x5.state & x6.state & x7.state & x8.state & x9.state & x10.state & x11.state & x12.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11, KDeferred x12, KDeferred x13) {
        if ((OK & x1.state & x2.state & x3.state & x4.state & x5.state & x6.state & x7.state & x8.state & x9.state & x10.state & x11.state & x12.state & x13.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11,  KDeferred x12, KDeferred x13, KDeferred x14) {
        if ((OK & x1.state & x2.state & x3.state & x4.state & x5.state & x6.state & x7.state & x8.state & x9.state & x10.state & x11.state & x12.state & x13.state & x14.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11, KDeferred x12, KDeferred x13, KDeferred x14, KDeferred x15) {
        if ((OK & x1.state & x2.state & x3.state & x4.state & x5.state & x6.state & x7.state & x8.state & x9.state & x10.state & x11.state & x12.state & x13.state & x14.state & x15.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11, KDeferred x12, KDeferred x13, KDeferred x14, KDeferred x15, KDeferred x16) {
        if ((OK & x1.state & x2.state & x3.state & x4.state & x5.state & x6.state & x7.state & x8.state & x9.state & x10.state & x11.state & x12.state & x13.state & x14.state & x15.state & x16.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void awaitArr(IDeferredListener ls, KDeferred[] ds, int len) {
        for (int i = len - 1; i >= 0; --i) {
            KDeferred d = ds[i];
            if (d.state != OK) {
                d.addListener(new Arr(i - 1, ls, ds));
                return;
            }
        }
        ls.onSuccess(null);
    }

    public static void awaitArr(IDeferredListener ls, KDeferred... ds) {
        for (int i = ds.length - 1; i >= 0; --i) {
            KDeferred d = ds[i];
            if (d.state != OK) {
                d.addListener(new Arr(i - 1, ls, ds));
                return;
            }
        }
        ls.onSuccess(null);
    }

    public static KDeferred[] createArr(long n) {
        return new KDeferred[(int) n];
    }

    public static void setArrItem(KDeferred[] a, long i, KDeferred d) {
        a[(int) i] = d;
    }

    public static void awaitIter(IDeferredListener ls, Iterator<KDeferred> ds) {
        if (ds.hasNext()) {
            new Iter(ds, ls).onSuccess(null);
        } else {
            ls.onSuccess(null);
        }
    }

    // Awaiters

    private static abstract class Lx implements IDeferredListener {

        protected final IDeferredListener ls;

        protected Lx(IDeferredListener ls) {
            this.ls = ls;
        }

        public Object onError(Object e) {
            ls.onError(e);
            return null;
        }
    }

    private static final class L1 extends Lx {

        private final KDeferred x1;

        private L1(
                IDeferredListener ls,
                KDeferred x1) {
            super(ls);
            this.x1 = x1;
        }

        public Object onSuccess(Object _x) {
            if (x1.state != OK) {
                x1.addListener(ls);
            } else {
                ls.onSuccess(null);
            }
            return null;
        }
    }

    private static final class L2 extends Lx {

        private final KDeferred x1;
        private final KDeferred x2;

        private L2(
                IDeferredListener ls,
                KDeferred x1,
                KDeferred x2) {
            super(ls);
            this.x1 = x1;
            this.x2 = x2;
        }

        public Object onSuccess(Object _x) {
            if (x2.state != OK) {
                x2.addListener(new L1(ls, x1));
            } else if (x1.state != OK) {
                x1.addListener(ls);
            } else {
                ls.onSuccess(null);
            }
            return null;
        }
    }

    private static final class L3 extends Lx {

        private final KDeferred x1;
        private final KDeferred x2;
        private final KDeferred x3;

        private L3(
                IDeferredListener ls,
                KDeferred x1,
                KDeferred x2,
                KDeferred x3) {
            super(ls);
            this.x1 = x1;
            this.x2 = x2;
            this.x3 = x3;
        }

        public Object onSuccess(Object _x) {
            if (x3.state != OK) {
                x3.addListener(new L2(ls, x1, x2));
            } else if (x2.state != OK) {
                x2.addListener(new L1(ls, x1));
            } else if (x1.state != OK) {
                x1.addListener(ls);
            } else {
                ls.onSuccess(null);
            }
            return null;
        }
    }

    private static class Arr extends Lx {

        private int i;
        private final KDeferred[] ds;

        private Arr(int i, IDeferredListener ls, KDeferred[] ds) {
            super(ls);
            this.i = i;
            this.ds = ds;
        }

        public Object onSuccess(Object x) {
            try {
                for (; i >= 0; --i) {
                    KDeferred d = ds[i];
                    if (d.state != OK) {
                        d.addListener(this);
                        return null;
                    }
                }
                ls.onSuccess(null);
            } catch (Throwable e) {
                KDeferred.logException(e);
            }
            return null;
        }
    }

    private static class Iter extends Lx {

        private final Iterator<KDeferred> da;

        private Iter(Iterator<KDeferred> da, IDeferredListener ls) {
            super(ls);
            this.da = da;
        }

        public Object onSuccess(Object x) {
            try {
                while (da.hasNext()) {
                    KDeferred d = da.next();
                    if (d.state != OK) {
                        d.addListener(this);
                        return null;
                    }
                }
                ls.onSuccess(null);
            } catch (Throwable e) {
                KDeferred.logException(e);
            }
            return null;
        }
    }
}