package knitty.javaimpl;

import clojure.lang.AFn;
import knitty.javaimpl.KDeferred.FailCallback;
import knitty.javaimpl.KDeferred.SuccCallback;
import manifold.deferred.IDeferred;
import manifold.deferred.IDeferredListener;
import manifold.deferred.IMutableDeferred;

public final class KDeferredAwaiter implements IDeferredListener {

    private final Object[] da;
    private final IDeferredListener ls;
    private int i;
    private IDeferred cd;

    private KDeferredAwaiter(Object[] da, IDeferredListener ls) {
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
            KDeferred.logException(e);
        }
        return null;
    }

    abstract static class AChainedListener implements IDeferredListener {
        private final IDeferredListener next;

        public AChainedListener(IDeferredListener next) {
            this.next = next;
        }

        public Object onError(Object x) {
            return next.onError(x);
        }
    }

    public static void awaitAll(IDeferredListener ls, Object... ds) {
        new KDeferredAwaiter(ds, ls).onSuccess(null);
    }

    public static void await(IDeferredListener ls) {
        ls.onSuccess(null);
    }

    private static void addListener(Object x, IDeferredListener ls) {
        if (x instanceof IMutableDeferred) {
            ((IMutableDeferred) x).addListener(ls);
        } else {
            ((IDeferred) x).onRealized(
                    new AFn() {
                        public Object invoke(Object x) {
                            return ls.onSuccess(x);
                        }
                    },
                    new AFn() {
                        public Object invoke(Object x) {
                            return ls.onError(x);
                        }
                    });
        }
    }

    public static void await(IDeferredListener ls, Object x1) {
        if (x1 instanceof IDeferred) {
           addListener(x1, ls);
        } else {
            ls.onSuccess(null);
        }
    }

    public static void await(IDeferredListener ls, Object x1, Object x2) {
        if (x2 instanceof IDeferred) {
            addListener(x2, new AChainedListener(ls) {
                public Object onSuccess(Object _x) {
                    await(ls, x1);
                    return null;
                }
            });
        } else {
            await(ls, x1);
        }
    }

    public static void await(IDeferredListener ls, Object x1, Object x2, Object x3) {
        if (x3 instanceof IDeferred) {
            addListener(x3, new AChainedListener(ls) {
                public Object onSuccess(Object _x) {
                    await(ls, x1, x2);
                    return null;
                }
            });
        } else {
            await(ls, x1, x2);
        }
    }

    public static void await(IDeferredListener ls, Object x1, Object x2, Object x3, Object x4) {
        if (x4 instanceof IDeferred) {
            addListener(x4, new AChainedListener(ls) {
                public Object onSuccess(Object _x) {
                    await(ls, x1, x2, x3);
                    return null;
                }
            });
        } else {
            await(ls, x1, x2, x3);
        }
    }

    public static void await(IDeferredListener ls, Object x1, Object x2, Object x3, Object x4, Object... xs) {
        IDeferredListener lsx = new AChainedListener(ls) {
            public Object onSuccess(Object _x) {
                await(ls, x1, x2, x3, x4);
                return null;
            }
        };
        awaitAll(lsx, xs);
    }
}