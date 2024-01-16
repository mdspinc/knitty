package knitty.javaimpl;

import java.util.Iterator;

import manifold.deferred.IDeferred;
import manifold.deferred.IDeferredListener;

public final class KAwaiter {

    private static int OK = KDeferred.STATE_SUCC;

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
        if ((OK & x1.state & x2.state & x3.state & x4.state) == 0) {
            awaitArr(ls, x1, x2, x3, x4);
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

    public static void awaitIterAny(IDeferredListener ls, Iterator<?> os) {
        awaitIter(ls, new OnlyKDs(os));
    }

    // Awaiters

    private static final class OnlyKDs implements Iterator<KDeferred> {

        private final Iterator<?> os;
        private KDeferred item;

        private OnlyKDs(Iterator<?> os) {
            this.os = os;
            this.advance();
        }

        @Override
        public boolean hasNext() {
            return item != null;
        }

        @Override
        public KDeferred next() {
            KDeferred r = item;
            advance();
            return r;
        }

        private void advance() {
            while (os.hasNext()) {
                Object x = os.next();
                if (x instanceof IDeferred) {
                    this.item = KDeferred.wrap(x);
                    return;
                }
            }
            this.item = null;
        }
    }

    private static final class L1 implements IDeferredListener {

        private final IDeferredListener ls;
        private final KDeferred x1;

        private L1(
                IDeferredListener ls,
                KDeferred x1) {
            this.ls = ls;
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

        public Object onError(Object e) {
            ls.onError(e);
            return null;
        }
    }

    private static final class L2 implements IDeferredListener {

        private final IDeferredListener ls;
        private final KDeferred x1;
        private final KDeferred x2;

        private L2(
                IDeferredListener ls,
                KDeferred x1,
                KDeferred x2) {
            this.ls = ls;
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

        public Object onError(Object e) {
            ls.onError(e);
            return null;
        }
    }

    private static class Arr implements IDeferredListener {

        private int i;
        private final IDeferredListener ls;
        private final KDeferred[] ds;

        private Arr(int i, IDeferredListener ls, KDeferred[] ds) {
            this.i = i;
            this.ls = ls;
            this.ds = ds;
        }

        public Object onError(Object e) {
            return ls.onError(e);
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

    private static class Iter implements IDeferredListener {

        private final Iterator<KDeferred> da;
        private final IDeferredListener ls;

        private Iter(Iterator<KDeferred> da, IDeferredListener ls) {
            this.da = da;
            this.ls = ls;
        }

        public Object onError(Object e) {
            return ls.onError(e);
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