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

    private static final class P8 extends Lx {

        private final KDeferred x1;
        private final KDeferred x2;
        private final KDeferred x3;
        private final KDeferred x4;
        private final KDeferred x5;
        private final KDeferred x6;
        private final KDeferred x7;
        private final KDeferred x8;

        private P8(
                IDeferredListener next,
                KDeferred x1,
                KDeferred x2,
                KDeferred x3,
                KDeferred x4,
                KDeferred x5,
                KDeferred x6,
                KDeferred x7,
                KDeferred x8) {
            super(next);
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
            await(next, x1, x2, x3, x4, x5, x6, x7, x8);
            return null;
        }
    }

    abstract static class Lx implements IDeferredListener {
        final IDeferredListener next;

        public Lx(IDeferredListener next) {
            this.next = next;
        }

        public Object onError(Object x) {
            return next.onError(x);
        }
    }

    public static void awaitIter(IDeferredListener ls, Iterator<KDeferred> ds) {
        new Iter(ds, ls).onSuccess(null);
    }

    public static void awaitArr(IDeferredListener ls, KDeferred... ds) {
        new Arr(ds, ls).onSuccess(null);
    }

    public static KDeferred[] createArr(int n) {
        return new KDeferred[n];
    }

    public static void setArrItem(KDeferred[] a, int i, KDeferred d) {
        a[i] = d;
    }

    public static void await(IDeferredListener ls) {
        ls.onSuccess(null);
    }

    public static void await(IDeferredListener ls, KDeferred x1) {
        if (x1.realized()) {
            ls.onSuccess(null);
        } else {
            x1.addListener(ls);
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2) {
        if (x2.realized()) {
            if (x1.realized()) {
                ls.onSuccess(null);
            } else {
                x1.addListener(ls);
            }
        } else {
            x2.addListener(new Lx(ls) {
                public Object onSuccess(Object _x) {
                    await(ls, x1);
                    return null;
                }
            });
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3) {
        if (x3.realized()) {
            if (x2.realized()) {
                if (x1.realized()) {
                    ls.onSuccess(null);
                } else {
                    x1.addListener(ls);
                }
            } else {
                await(ls, x1, x2);
            }
        } else {
            x3.addListener(new Lx(ls) {
                public Object onSuccess(Object _x) {
                    await(ls, x1, x2);
                    return null;
                }
            });
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4) {
        if (x4.realized()) {
            if (x3.realized()) {
                if (x2.realized()) {
                    if (x1.realized()) {
                        ls.onSuccess(null);
                    } else {
                        x1.addListener(ls);
                    }
                } else {
                    await(ls, x1, x2);
                }
            } else {
                await(ls, x1, x2, x3);
            }
        } else {
            x4.addListener(new Lx(ls) {
                public Object onSuccess(Object _x) {
                    await(ls, x1, x2, x3);
                    return null;
                }
            });
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5) {
        if (x5.realized()) {
            if (x4.realized()) {
                if (x3.realized()) {
                    if (x2.realized()) {
                        if (x1.realized()) {
                            ls.onSuccess(null);
                        } else {
                            x1.addListener(ls);
                        }
                    } else {
                        await(ls, x1, x2);
                    }
                } else {
                    await(ls, x1, x2, x3);
                }
            } else {
                await(ls, x1, x2, x3, x4);
            }
        } else {
            x5.addListener(new Lx(ls) {
                public Object onSuccess(Object _x) {
                    await(ls, x1, x2, x3, x4);
                    return null;
                }
            });
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6) {
        if (x6.realized()) {
            if (x5.realized()) {
                if (x4.realized()) {
                    if (x3.realized()) {
                        if (x2.realized()) {
                            if (x1.realized()) {
                                ls.onSuccess(null);
                            } else {
                                x1.addListener(ls);
                            }
                        } else {
                            await(ls, x1, x2);
                        }
                    } else {
                        await(ls, x1, x2, x3);
                    }
                } else {
                    await(ls, x1, x2, x3, x4);
                }
            } else {
                await(ls, x1, x2, x3, x4, x5);
            }
        } else {
            x6.addListener(new Lx(ls) {
                public Object onSuccess(Object _x) {
                    await(ls, x1, x2, x3, x4, x5);
                    return null;
                }
            });
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7) {
        if (x7.realized()) {
            if (x6.realized()) {
                if (x5.realized()) {
                    if (x4.realized()) {
                        if (x3.realized()) {
                            if (x2.realized()) {
                                if (x1.realized()) {
                                    ls.onSuccess(null);
                                } else {
                                    x1.addListener(ls);
                                }
                            } else {
                                await(ls, x1, x2);
                            }
                        } else {
                            await(ls, x1, x2, x3);
                        }
                    } else {
                        await(ls, x1, x2, x3, x4);
                    }
                } else {
                    await(ls, x1, x2, x3, x4, x5);
                }
            } else {
                await(ls, x1, x2, x3, x4, x5, x6);
            }
        } else {
            x7.addListener(new Lx(ls) {
                public Object onSuccess(Object _x) {
                    await(ls, x1, x2, x3, x4, x5, x6);
                    return null;
                }
            });
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8) {
        if (x8.realized()) {
            if (x7.realized()) {
                if (x6.realized()) {
                    if (x5.realized()) {
                        if (x4.realized()) {
                            if (x3.realized()) {
                                if (x2.realized()) {
                                    if (x1.realized()) {
                                        ls.onSuccess(null);
                                    } else {
                                        x1.addListener(ls);
                                    }
                                } else {
                                    await(ls, x1, x2);
                                }
                            } else {
                                await(ls, x1, x2, x3);
                            }
                        } else {
                            await(ls, x1, x2, x3, x4);
                        }
                    } else {
                        await(ls, x1, x2, x3, x4, x5);
                    }
                } else {
                    await(ls, x1, x2, x3, x4, x5, x6);
                }
            } else {
                await(ls, x1, x2, x3, x4, x5, x6, x7);
            }
        } else {
            x8.addListener(new Lx(ls) {
                public Object onSuccess(Object _x) {
                    await(ls, x1, x2, x3, x4, x5, x6, x7);
                    return null;
                }
            });
        }
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9) {
        await(new P8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9);
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10) {
        await(new P8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10);
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11) {
        await(new P8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11);
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11,
            KDeferred x12) {
        await(new P8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12);
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11,
            KDeferred x12, KDeferred x13) {
        await(new P8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13);
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11,
            KDeferred x12, KDeferred x13, KDeferred x14) {
        await(new P8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13, x14);
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11,
            KDeferred x12, KDeferred x13, KDeferred x14, KDeferred x15) {
        await(new P8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13, x14, x15);
    }

    public static void await(IDeferredListener ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4,
            KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11,
            KDeferred x12, KDeferred x13, KDeferred x14, KDeferred x15, KDeferred x16) {
        await(new P8(ls, x1, x2, x3, x4, x5, x6, x7, x8), x9, x10, x11, x12, x13, x14, x15, x16);
    }
}