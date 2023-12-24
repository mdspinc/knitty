package knitty.javaimpl;

import java.util.Iterator;
import manifold.deferred.IDeferredListener;

public final class KAwaiter {

    private static class Arr implements IDeferredListener {

        private final KDeferred[] da;
        private final IDeferredListener ls;
        private int i;

        private Arr(KDeferred[] da, IDeferredListener ls) {
            this.da = da;
            this.ls = ls;
            this.i = da.length;
        }

        public Object onError(Object e) {
            return ls.onError(e);
        }

        public Object onSuccess(Object x) {
            try {
                while (i > 0) {
                    if (da[--i].addAwaitListener(this)) {
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
                    if (d.addAwaitListener(this)) {
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

    public static void awaitIter(IDeferredListener ls, Iterator<KDeferred> ds) {
        new Iter(ds, ls).onSuccess(null);
    }

    public static void awaitArr(IDeferredListener ls, KDeferred... ds) {
        new Arr(ds, ls).onSuccess(null);
    }

    public static KDeferred[] createArr(long n) {
        return new KDeferred[(int) n];
    }

    public static void setArrItem(KDeferred[] a, long i, KDeferred d) {
        a[(int) i] = d;
    }

    public static void await(IDeferredListener ls) {
        ls.onSuccess(null);
    }

    public static void await(IDeferredListener ls, KDeferred x1) {
        if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2) {
        if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3) {
        if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4) {
        if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5) {
        if (x5.state != KDeferred.STATE_SUCC) {
            x5.addListener(new L4(ls, x1, x2, x3, x4));
        } else if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6) {
        if (x6.state != KDeferred.STATE_SUCC) {
            x6.addListener(new L5(ls, x1, x2, x3, x4, x5));
        } else if (x5.state != KDeferred.STATE_SUCC) {
            x5.addListener(new L4(ls, x1, x2, x3, x4));
        } else if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7) {
        if (x7.state != KDeferred.STATE_SUCC) {
            x7.addListener(new L6(ls, x1, x2, x3, x4, x5, x6));
        } else if (x6.state != KDeferred.STATE_SUCC) {
            x6.addListener(new L5(ls, x1, x2, x3, x4, x5));
        } else if (x5.state != KDeferred.STATE_SUCC) {
            x5.addListener(new L4(ls, x1, x2, x3, x4));
        } else if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8) {
        if (x8.state != KDeferred.STATE_SUCC) {
            x8.addListener(new L7(ls, x1, x2, x3, x4, x5, x6, x7));
        } else if (x7.state != KDeferred.STATE_SUCC) {
            x7.addListener(new L6(ls, x1, x2, x3, x4, x5, x6));
        } else if (x6.state != KDeferred.STATE_SUCC) {
            x6.addListener(new L5(ls, x1, x2, x3, x4, x5));
        } else if (x5.state != KDeferred.STATE_SUCC) {
            x5.addListener(new L4(ls, x1, x2, x3, x4));
        } else if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9) {
        if (x9.state != KDeferred.STATE_SUCC) {
            x9.addListener(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8));
        } else if (x8.state != KDeferred.STATE_SUCC) {
            x8.addListener(new L7(ls, x1, x2, x3, x4, x5, x6, x7));
        } else if (x7.state != KDeferred.STATE_SUCC) {
            x7.addListener(new L6(ls, x1, x2, x3, x4, x5, x6));
        } else if (x6.state != KDeferred.STATE_SUCC) {
            x6.addListener(new L5(ls, x1, x2, x3, x4, x5));
        } else if (x5.state != KDeferred.STATE_SUCC) {
            x5.addListener(new L4(ls, x1, x2, x3, x4));
        } else if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10) {
        if (x10.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10);
        } else if (x9.state != KDeferred.STATE_SUCC) {
            x9.addListener(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8));
        } else if (x8.state != KDeferred.STATE_SUCC) {
            x8.addListener(new L7(ls, x1, x2, x3, x4, x5, x6, x7));
        } else if (x7.state != KDeferred.STATE_SUCC) {
            x7.addListener(new L6(ls, x1, x2, x3, x4, x5, x6));
        } else if (x6.state != KDeferred.STATE_SUCC) {
            x6.addListener(new L5(ls, x1, x2, x3, x4, x5));
        } else if (x5.state != KDeferred.STATE_SUCC) {
            x5.addListener(new L4(ls, x1, x2, x3, x4));
        } else if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11) {
        if (x11.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11);
        } else if (x10.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10);
        } else if (x9.state != KDeferred.STATE_SUCC) {
            x9.addListener(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8));
        } else if (x8.state != KDeferred.STATE_SUCC) {
            x8.addListener(new L7(ls, x1, x2, x3, x4, x5, x6, x7));
        } else if (x7.state != KDeferred.STATE_SUCC) {
            x7.addListener(new L6(ls, x1, x2, x3, x4, x5, x6));
        } else if (x6.state != KDeferred.STATE_SUCC) {
            x6.addListener(new L5(ls, x1, x2, x3, x4, x5));
        } else if (x5.state != KDeferred.STATE_SUCC) {
            x5.addListener(new L4(ls, x1, x2, x3, x4));
        } else if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11,
            KDeferred x12) {
        if (x12.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12);
        } else if (x11.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11);
        } else if (x10.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10);
        } else if (x9.state != KDeferred.STATE_SUCC) {
            x9.addListener(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8));
        } else if (x8.state != KDeferred.STATE_SUCC) {
            x8.addListener(new L7(ls, x1, x2, x3, x4, x5, x6, x7));
        } else if (x7.state != KDeferred.STATE_SUCC) {
            x7.addListener(new L6(ls, x1, x2, x3, x4, x5, x6));
        } else if (x6.state != KDeferred.STATE_SUCC) {
            x6.addListener(new L5(ls, x1, x2, x3, x4, x5));
        } else if (x5.state != KDeferred.STATE_SUCC) {
            x5.addListener(new L4(ls, x1, x2, x3, x4));
        } else if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11,
            KDeferred x12, KDeferred x13) {
        if (x13.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13);
        } else if (x12.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12);
        } else if (x11.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11);
        } else if (x10.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10);
        } else if (x9.state != KDeferred.STATE_SUCC) {
            x9.addListener(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8));
        } else if (x8.state != KDeferred.STATE_SUCC) {
            x8.addListener(new L7(ls, x1, x2, x3, x4, x5, x6, x7));
        } else if (x7.state != KDeferred.STATE_SUCC) {
            x7.addListener(new L6(ls, x1, x2, x3, x4, x5, x6));
        } else if (x6.state != KDeferred.STATE_SUCC) {
            x6.addListener(new L5(ls, x1, x2, x3, x4, x5));
        } else if (x5.state != KDeferred.STATE_SUCC) {
            x5.addListener(new L4(ls, x1, x2, x3, x4));
        } else if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11,
            KDeferred x12, KDeferred x13, KDeferred x14) {
        if (x14.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13, x14);
        } else if (x13.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13);
        } else if (x12.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12);
        } else if (x11.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11);
        } else if (x10.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10);
        } else if (x9.state != KDeferred.STATE_SUCC) {
            x9.addListener(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8));
        } else if (x8.state != KDeferred.STATE_SUCC) {
            x8.addListener(new L7(ls, x1, x2, x3, x4, x5, x6, x7));
        } else if (x7.state != KDeferred.STATE_SUCC) {
            x7.addListener(new L6(ls, x1, x2, x3, x4, x5, x6));
        } else if (x6.state != KDeferred.STATE_SUCC) {
            x6.addListener(new L5(ls, x1, x2, x3, x4, x5));
        } else if (x5.state != KDeferred.STATE_SUCC) {
            x5.addListener(new L4(ls, x1, x2, x3, x4));
        } else if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11,
            KDeferred x12, KDeferred x13, KDeferred x14, KDeferred x15) {
        if (x15.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13, x14, x15);
        } else if (x14.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13, x14);
        } else if (x13.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13);
        } else if (x12.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12);
        } else if (x11.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11);
        } else if (x10.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10);
        } else if (x9.state != KDeferred.STATE_SUCC) {
            x9.addListener(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8));
        } else if (x8.state != KDeferred.STATE_SUCC) {
            x8.addListener(new L7(ls, x1, x2, x3, x4, x5, x6, x7));
        } else if (x7.state != KDeferred.STATE_SUCC) {
            x7.addListener(new L6(ls, x1, x2, x3, x4, x5, x6));
        } else if (x6.state != KDeferred.STATE_SUCC) {
            x6.addListener(new L5(ls, x1, x2, x3, x4, x5));
        } else if (x5.state != KDeferred.STATE_SUCC) {
            x5.addListener(new L4(ls, x1, x2, x3, x4));
        } else if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11,
            KDeferred x12, KDeferred x13, KDeferred x14, KDeferred x15, KDeferred x16) {
        if (x16.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13, x14, x15, x16);
        } else if (x15.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13, x14, x15);
        } else if (x14.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13, x14);
        } else if (x13.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13);
        } else if (x12.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12);
        } else if (x11.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11);
        } else if (x10.state != KDeferred.STATE_SUCC) {
            await(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10);
        } else if (x9.state != KDeferred.STATE_SUCC) {
            x9.addListener(new L8(ls, x1, x2, x3, x4, x5, x6, x7, x8));
        } else if (x8.state != KDeferred.STATE_SUCC) {
            x8.addListener(new L7(ls, x1, x2, x3, x4, x5, x6, x7));
        } else if (x7.state != KDeferred.STATE_SUCC) {
            x7.addListener(new L6(ls, x1, x2, x3, x4, x5, x6));
        } else if (x6.state != KDeferred.STATE_SUCC) {
            x6.addListener(new L5(ls, x1, x2, x3, x4, x5));
        } else if (x5.state != KDeferred.STATE_SUCC) {
            x5.addListener(new L4(ls, x1, x2, x3, x4));
        } else if (x4.state != KDeferred.STATE_SUCC) {
            x4.addListener(new L3(ls, x1, x2, x3));
        } else if (x3.state != KDeferred.STATE_SUCC) {
            x3.addListener(new L2(ls, x1, x2));
        } else if (x2.state != KDeferred.STATE_SUCC) {
            x2.addListener(new L1(ls, x1));
        } else if (x1.state != KDeferred.STATE_SUCC) {
            x1.addListener(ls);
        } else {
            ls.onSuccess(null);
        }
    }

    // Awaiters

    private static final class L1 implements IDeferredListener {

        private final IDeferredListener ls;
        private final KDeferred x1;

        private L1(
            IDeferredListener ls,
            KDeferred x1
        ) {
            this.ls = ls;
            this.x1 = x1;
        }

        public Object onSuccess(Object _x) {
            await(ls, x1);
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
            KDeferred x2
        ) {
            this.ls = ls;
            this.x1 = x1;
            this.x2 = x2;
        }

        public Object onSuccess(Object _x) {
            await(ls, x1, x2);
            return null;
        }

        public Object onError(Object e) {
            ls.onError(e);
            return null;
        }
    }

    private static final class L3 implements IDeferredListener {

        private final IDeferredListener ls;
        private final KDeferred x1;
        private final KDeferred x2;
        private final KDeferred x3;

        private L3(
            IDeferredListener ls,
            KDeferred x1,
            KDeferred x2,
            KDeferred x3
        ) {
            this.ls = ls;
            this.x1 = x1;
            this.x2 = x2;
            this.x3 = x3;
        }

        public Object onSuccess(Object _x) {
            await(ls, x1, x2, x3);
            return null;
        }

        public Object onError(Object e) {
            ls.onError(e);
            return null;
        }
    }

    private static final class L4 implements IDeferredListener {

        private final IDeferredListener ls;
        private final KDeferred x1;
        private final KDeferred x2;
        private final KDeferred x3;
        private final KDeferred x4;

        private L4(
            IDeferredListener ls,
            KDeferred x1,
            KDeferred x2,
            KDeferred x3,
            KDeferred x4
        ) {
            this.ls = ls;
            this.x1 = x1;
            this.x2 = x2;
            this.x3 = x3;
            this.x4 = x4;
        }

        public Object onSuccess(Object _x) {
            await(ls, x1, x2, x3, x4);
            return null;
        }

        public Object onError(Object e) {
            ls.onError(e);
            return null;
        }
    }

    private static final class L5 implements IDeferredListener {

        private final IDeferredListener ls;
        private final KDeferred x1;
        private final KDeferred x2;
        private final KDeferred x3;
        private final KDeferred x4;
        private final KDeferred x5;

        private L5(
            IDeferredListener ls,
            KDeferred x1,
            KDeferred x2,
            KDeferred x3,
            KDeferred x4,
            KDeferred x5
        ) {
            this.ls = ls;
            this.x1 = x1;
            this.x2 = x2;
            this.x3 = x3;
            this.x4 = x4;
            this.x5 = x5;
        }

        public Object onSuccess(Object _x) {
            await(ls, x1, x2, x3, x4, x5);
            return null;
        }

        public Object onError(Object e) {
            ls.onError(e);
            return null;
        }
    }

    private static final class L6 implements IDeferredListener {

        private final IDeferredListener ls;
        private final KDeferred x1;
        private final KDeferred x2;
        private final KDeferred x3;
        private final KDeferred x4;
        private final KDeferred x5;
        private final KDeferred x6;

        private L6(
            IDeferredListener ls,
            KDeferred x1,
            KDeferred x2,
            KDeferred x3,
            KDeferred x4,
            KDeferred x5,
            KDeferred x6
        ) {
            this.ls = ls;
            this.x1 = x1;
            this.x2 = x2;
            this.x3 = x3;
            this.x4 = x4;
            this.x5 = x5;
            this.x6 = x6;
        }

        public Object onSuccess(Object _x) {
            await(ls, x1, x2, x3, x4, x5, x6);
            return null;
        }

        public Object onError(Object e) {
            ls.onError(e);
            return null;
        }
    }

    private static final class L7 implements IDeferredListener {

        private final IDeferredListener ls;
        private final KDeferred x1;
        private final KDeferred x2;
        private final KDeferred x3;
        private final KDeferred x4;
        private final KDeferred x5;
        private final KDeferred x6;
        private final KDeferred x7;

        private L7(
            IDeferredListener ls,
            KDeferred x1,
            KDeferred x2,
            KDeferred x3,
            KDeferred x4,
            KDeferred x5,
            KDeferred x6,
            KDeferred x7
        ) {
            this.ls = ls;
            this.x1 = x1;
            this.x2 = x2;
            this.x3 = x3;
            this.x4 = x4;
            this.x5 = x5;
            this.x6 = x6;
            this.x7 = x7;
        }

        public Object onSuccess(Object _x) {
            await(ls, x1, x2, x3, x4, x5, x6, x7);
            return null;
        }

        public Object onError(Object e) {
            ls.onError(e);
            return null;
        }
    }

    private static final class L8 implements IDeferredListener {

        private final IDeferredListener ls;
        private final KDeferred x1;
        private final KDeferred x2;
        private final KDeferred x3;
        private final KDeferred x4;
        private final KDeferred x5;
        private final KDeferred x6;
        private final KDeferred x7;
        private final KDeferred x8;

        private L8(
            IDeferredListener ls,
            KDeferred x1,
            KDeferred x2,
            KDeferred x3,
            KDeferred x4,
            KDeferred x5,
            KDeferred x6,
            KDeferred x7,
            KDeferred x8
        ) {
            this.ls = ls;
            this.x1 = x1;
            this.x2 = x2;
            this.x3 = x3;
            this.x4 = x4;
            this.x5 = x5;
            this.x6 = x6;
            this.x7 = x7;
            this.x8 = x8;
        }

        public Object onSuccess(Object _x) {
            await(ls, x1, x2, x3, x4, x5, x6, x7, x8);
            return null;
        }

        public Object onError(Object e) {
            ls.onError(e);
            return null;
        }
    }

}