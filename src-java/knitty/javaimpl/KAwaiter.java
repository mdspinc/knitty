package knitty.javaimpl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Iterator;

import clojure.lang.AFn;
import manifold.deferred.IDeferred;

public final class KAwaiter {

    private static final class Ls extends AListener {

        private final KAwaiter ka;

        Ls(KAwaiter ka) {
            this.ka = ka;
        }

        public void success(Object x) {
            if ((int) CNT.getAndAddAcquire(this.ka, (int) -1) == 1) {
                this.ka.ls.invoke();
            }
        }

        public void error(Object e) {
            if ((int) CNT.getAndSetAcquire(this.ka, (int) -1) > 0) {
                this.ka.ls.invoke(e);
            }
        }
    }

    private static final class L0 extends AListener {
        final AFn ls;

        L0(AFn ls) {
            this.ls = ls;
        }

        public void error(Object e) {
            ls.invoke(e);
        }

        public void success(Object x) {
            ls.invoke();
        }
    }

    private final AFn ls;
    private int acnt = Integer.MAX_VALUE;
    private int _cnt = Integer.MAX_VALUE;

    private static final VarHandle CNT;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            CNT = l.findVarHandle(KAwaiter.class, "_cnt", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private KAwaiter(AFn ls) {
        this.ls = ls;
    }

    public boolean await() {
        return (acnt == Integer.MAX_VALUE) || ((int) CNT.getAndAddRelease(KAwaiter.this, -acnt) == acnt);
    }

    private static boolean failed(KAwaiter ka) {
        return ka != null && ((int) CNT.getOpaque(ka)) <= 0;
    }

    public static KAwaiter start(AFn ls) {
        return new KAwaiter(ls);
    }

    private static KAwaiter start(KAwaiter ka, AFn ls) {
        if (ka == null) {
            return new KAwaiter(ls);
        } else if (ka.acnt <= 4) {
            throw new IllegalStateException("too much deferreds are awaited");
        } else {
            return ka;
        }
    }

    private void with0(KDeferred d1) {
        this.acnt -= 1;
        d1.listen(new Ls(this));
    }

    public static KAwaiter with(KAwaiter ka, AFn ls, KDeferred x1) {
        int s1 = x1.succeeded;
        if (s1 == 1) {
            return ka;
        } else {
            ka = start(ka, ls);
            ka.with0(x1);
            return ka;
        }
    }

    public static KAwaiter with(KAwaiter ka, AFn ls, KDeferred x1, KDeferred x2) {
        int s1 = x1.succeeded;
        int s2 = x2.succeeded;
        if ((s1 & s2) == 0) {
            ka = start(ka, ls);
            switch ((s2 << 1) | s1) {
                case 0b00: ka.with0(x1);  // 2 1
                case 0b01: x1 = x2;       // 2
                case 0b10: ka.with0(x1);  // 1
            }
        }
        return ka;
    }

    public static KAwaiter with(KAwaiter ka, AFn ls, KDeferred x1, KDeferred x2, KDeferred x3) {
        int s1 = x1.succeeded;
        int s2 = x2.succeeded;
        int s3 = x3.succeeded;
        if ((s1 & s2 & s3) == 0) {
            ka = start(ka, ls);
            switch ((s3 << 2) | (s2 << 1) | s1) {
                case 0b000: ka.with0(x1);  // 3 2 1
                case 0b001: x1 = x3;       // 3 2
                case 0b100: x3 = x2;       // 2 1
                case 0b010: ka.with0(x1);  // 3 1
                case 0b011: x2 = x3;       // 3
                case 0b101: x1 = x2;       // 2
                case 0b110: ka.with0(x1);  // 1
            }
        }
        return ka;
    }

    public static KAwaiter with(KAwaiter ka, AFn ls, KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4) {
        int s1 = x1.succeeded;
        int s2 = x2.succeeded;
        int s3 = x3.succeeded;
        int s4 = x4.succeeded;
        if ((s1 & s2 & s3 & s4) == 0) {
            ka = start(ka, ls);
            switch ((s4 << 3) | (s3 << 2) | (s2 << 1) | s1) {
                case 0b0000: ka.with0(x1);  // 4 3 2 1
                case 0b0001: x1 = x2;       // 4 3 2
                case 0b0010: x2 = x4;       // 4 3 1
                case 0b1000: x4 = x3;       // 3 2 1
                case 0b0100: ka.with0(x1);  // 4 2 1
                case 0b0101: x1 = x4;       // 4 2
                case 0b1100: x3 = x2;       // 2 1
                case 0b1010: x2 = x1;       // 1 3
                case 0b1001: x4 = x2;       // 3 2
                case 0b0011: x1 = x3;       // 4 3
                case 0b0110: ka.with0(x1);  // 4 1
                case 0b0111: x3 = x4;       // 4
                case 0b1011: x2 = x3;       // 3
                case 0b1101: x1 = x2;       // 2
                case 0b1110: ka.with0(x1);  // 1
            }
        }
        return ka;
    }

    public static boolean await(KAwaiter ka) {
        return ka == null || ka.await();
    }

    public static boolean await1(AFn ls, KDeferred x1) {
        if (x1.succeeded == 1) {
            return true;
        } else {
            x1.listen(new L0(ls));
            return false;
        }
    }

    public static boolean awaitIter(AFn ls, Iterator<?> ds) {
        KAwaiter ka = null;
        while (ds.hasNext() && !failed(ka)) {
            Object d = ds.next();
            if (d instanceof IDeferred) {
                KDeferred kd = KDeferred.wrapDeferred((IDeferred) d);
                ka = with(ka, ls, kd);
            }
        }
        return await(ka);
    }
}