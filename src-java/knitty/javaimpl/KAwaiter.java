package knitty.javaimpl;

import java.util.Iterator;
import clojure.lang.AFn;

public final class KAwaiter {

    public static boolean await(AFn ls) {
        return true;
    }

    public static boolean await(AFn ls, KDeferred x1) {
        if (x1.state == 1) {
            return true;
        } else {
            return !x1.listen0(new L0(ls));
        }
    }

    private static final byte mixStates(KDeferred x1, KDeferred x2) {
        return (byte) (((x1.state & 1) << 1) | (x2.state & 1));
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2) {
        switch (mixStates(x1, x2)) {
            case 0b00: x2.listen(new L1(ls, x1)); return false;
            case 0b01: x2 = x1;
            case 0b10: x2.listen(new L0(ls)); return false;
            case 0b11: return true;
        }
        return true;
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3) {
        switch (mixStates(x2, x3)) {
            case 0b00: x3.listen(new L2(ls, x1, x2)); return false;
            case 0b01: x3 = x2;
            case 0b10: x3.listen(new L1(ls, x1)); return false;
            case 0b11: return await(ls, x1);
        }
        return true;
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4) {
        switch (mixStates(x3, x4)) {
            case 0b00: x4.listen(new L3(ls, x1, x2, x3)); return false;
            case 0b01: x4 = x3;
            case 0b10: x4.listen(new L2(ls, x1, x2)); return false;
            case 0b11: return await(ls, x1, x2);
        }
        return true;
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5) {
        switch (mixStates(x4, x5)) {
            case 0b00: x5.listen(new L4(ls, x1, x2, x3, x4)); return false;
            case 0b01: x5 = x4;
            case 0b10: x5.listen(new L3(ls, x1, x2, x3)); return false;
            case 0b11: return await(ls, x1, x2, x3);
        }
        return true;
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6) {
        switch (mixStates(x5, x6)) {
            case 0b00: return awaitArr(ls, x1, x2, x3, x4, x5, x6);
            case 0b01: x6 = x5;
            case 0b10: x6.listen(new L4(ls, x1, x2, x3, x4)); return false;
            case 0b11: return await(ls, x1, x2, x3, x4);
        }
        return true;
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7) {
        if ((x1.state & x2.state & x3.state & x4.state) == 1) {
            return await(ls, x5, x6, x7);
        } else {
            return awaitArr(ls, x1, x2, x3, x4, x5, x6, x7);
        }
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8) {
        if ((x1.state & x2.state & x3.state & x4.state) == 1) {
            return await(ls, x5, x6, x7, x8);
        } else {
            return awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8);
        }
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9) {
        if ((x1.state & x2.state & x3.state & x4.state) == 1) {
            return await(ls, x5, x6, x7, x8, x9);
        } else {
            return awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9);
        }
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10) {
        if ((x1.state & x2.state & x3.state & x4.state) == 1) {
            return await(ls, x5, x6, x7, x8, x9, x10);
        } else {
            return awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10);
        }
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11) {
        if ((x1.state & x2.state & x3.state & x4.state) == 1) {
            return await(ls, x5, x6, x7, x8, x9, x10, x11);
        } else {
            return awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11);
        }
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11, KDeferred x12) {
        if ((x1.state & x2.state & x3.state & x4.state) == 1) {
            return await(ls, x5, x6, x7, x8, x9, x10, x11, x12);
        } else {
            return awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12);
        }
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11, KDeferred x12, KDeferred x13) {
        if ((x1.state & x2.state & x3.state & x4.state) == 1) {
            return await(ls, x5, x6, x7, x8, x9, x10, x11, x12, x13);
        } else {
            return awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13);
        }
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11,  KDeferred x12, KDeferred x13, KDeferred x14) {
        if ((x1.state & x2.state & x3.state & x4.state) == 1) {
            return await(ls, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14);
        } else {
            return awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14);
        }
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11, KDeferred x12, KDeferred x13, KDeferred x14, KDeferred x15) {
        if ((x1.state & x2.state & x3.state & x4.state) == 1) {
            return await(ls, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15);
        } else {
            return awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15);
        }
    }

    public static boolean await(AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4, KDeferred x5, KDeferred x6, KDeferred x7, KDeferred x8, KDeferred x9, KDeferred x10, KDeferred x11, KDeferred x12, KDeferred x13, KDeferred x14, KDeferred x15, KDeferred x16) {
        if ((x1.state & x2.state & x3.state & x4.state) == 1) {
            return await(ls, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16);
        } else {
            return awaitArr(ls, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16);
        }
    }

    public static boolean awaitArr(AFn ls, KDeferred[] ds, int len) {
        for (int i = len - 1; i >= 0; --i) {
            KDeferred d = ds[i];
            if (d.state != 1) {
                d.listen(new Arr(i - 1, ls, ds));
                return false;
            }
        }
        return true;
    }

    public static boolean awaitArr(AFn ls, KDeferred... ds) {
        for (int i = ds.length - 1; i >= 0; --i) {
            KDeferred d = ds[i];
            if (d.state != 1) {
                d.listen(new Arr(i - 1, ls, ds));
                return false;
            }
        }
        return true;
    }

    public static KDeferred[] createArr(long n) {
        return new KDeferred[(int) n];
    }

    public static void setArrItem(KDeferred[] a, long i, KDeferred d) {
        a[(int) i] = d;
    }

    public static void awaitIter(AFn ls, Iterator<KDeferred> ds) {
        if (ds.hasNext()) {
            new Iter(ds, ls).success(null);
        } else {
            ls.invoke();
        }
    }

    // Awaiters

    private static abstract class Lx extends AListener {

        final AFn ls;

        Lx(AFn ls) {
            this.ls = ls;
        }

        public void error(Object e) {
            ls.invoke(e);
        }
    }

    private static final class L0 extends Lx {

        L0(AFn ls) {
            super(ls);
        }

        public void success(Object x) {
            ls.invoke();
        }
    }

    private static final class L1 extends Lx {

        private final KDeferred x1;

        L1(
                AFn ls,
                KDeferred x1) {
            super(ls);
            this.x1 = x1;
        }

        public void success(Object _x) {
            if (await(ls, x1)) {
                ls.invoke();
            }
        }
    }

    private static final class L2 extends Lx {

        private final KDeferred x1;
        private final KDeferred x2;

        L2(
                AFn ls,
                KDeferred x1,
                KDeferred x2) {
            super(ls);
            this.x1 = x1;
            this.x2 = x2;
        }

        public void success(Object _x) {
            if (await(ls, x1, x2)) {
                ls.invoke();
            }
        }
    }

    private static final class L3 extends Lx {

        private final KDeferred x1;
        private final KDeferred x2;
        private final KDeferred x3;

        L3(
                AFn ls,
                KDeferred x1,
                KDeferred x2,
                KDeferred x3) {
            super(ls);
            this.x1 = x1;
            this.x2 = x2;
            this.x3 = x3;
        }

        public void success(Object _x) {
            if (await(ls, x1, x2, x3)) {
                ls.invoke();
            }
        }
    }

    private static final class L4 extends Lx {

        private final KDeferred x1;
        private final KDeferred x2;
        private final KDeferred x3;
        private final KDeferred x4;

        L4(
                AFn ls,
                KDeferred x1,
                KDeferred x2,
                KDeferred x3,
                KDeferred x4) {
            super(ls);
            this.x1 = x1;
            this.x2 = x2;
            this.x3 = x3;
            this.x4 = x4;
        }

        public void success(Object _x) {
            if (await(ls, x1, x2, x3, x4)) {
                ls.invoke();
            }
        }
    }

    private static final Exception EXPECTED_ERR = new IllegalStateException("kdeferred expected to be in error state");
    static {
        EXPECTED_ERR.setStackTrace(new StackTraceElement[0]);
    }

    private static class Arr extends Lx {

        private final KDeferred[] ds;
        private int i;

        Arr(int i, AFn ls, KDeferred[] ds) {
            super(ls);
            this.i = i;
            this.ds = ds;
        }

        public void success(Object x) {
            try {
                for (; i >= 0; --i) {
                    KDeferred d = ds[i];
                    if (d.state != 1) {
                        if (d.listen0(this)) {
                            return;
                        } else if (d.state != 1) {
                            this.error(d.errorValue(EXPECTED_ERR));
                        }
                    }
                }
                ls.invoke();
            } catch (Throwable e) {
                KDeferred.logError(e, "error in awaiter callback");
            }
        }
    }

    private static class Iter extends Lx {

        private final Iterator<KDeferred> da;

        Iter(Iterator<KDeferred> da, AFn ls) {
            super(ls);
            this.da = da;
        }

        public void success(Object x) {
            try {
                while (da.hasNext()) {
                    KDeferred d = da.next();
                    if (d.state != 1) {
                        if (d.listen0(this)) {
                            return;
                        } else if (d.state != 1) {
                            this.error(d.errorValue(EXPECTED_ERR));
                        }
                    }
                }
                ls.invoke();
            } catch (Throwable e) {
                KDeferred.logError(e, "error in awaiter callback");
            }
        }
    }
}