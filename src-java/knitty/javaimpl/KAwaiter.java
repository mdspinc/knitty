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

    private boolean isDone() {
        return ((int) CNT.getOpaque(this)) <= 0;
    }

    public static KAwaiter start(AFn ls) {
        return new KAwaiter(ls);
    }

    private void with0(KDeferred d1) {
        if (this.acnt <= 0) {
            throw new IllegalStateException("more than 2147483647 deferreds are awaited");
        }
        this.acnt -= 1;
        d1.listen(new Ls(this));
    }

    public void with(KDeferred x1) {
        if(x1.state() != 1) {
            with0(x1);
        }
    }

    public void with(KDeferred x1, KDeferred x2) {
        switch ((byte) (((x1.state() & 1) << 1) | (x2.state() & 1))) {
            case 0b00: with0(x2);
            case 0b01: x2 = x1;
            case 0b10: with0(x2);
            // case 0b11:
        }
    }

    public void with(KDeferred x1, KDeferred x2, KDeferred x3) {
        switch ((byte) (((x1.state() & 1) << 1) | (x2.state() & 1))) {
            case 0b00: with0(x2);
            case 0b01: x2 = x1;
            case 0b10: with0(x2);
            // case 0b11:
        }
        if (x3.state() != 1) {
            with0(x3);
        }
    }

    public void with(KDeferred x1, KDeferred x2, KDeferred x3, KDeferred x4) {
        switch ((byte) (((x1.state() & 1) << 1) | (x2.state() & 1))) {
            case 0b00: with0(x2);
            case 0b01: x2 = x1;
            case 0b10: with0(x2);
            // case 0b11:
        }
        switch ((byte) (((x3.state() & 1) << 1) | (x4.state() & 1))) {
            case 0b00: with0(x4);
            case 0b01: x4 = x3;
            case 0b10: with0(x4);
            // case 0b11:
        }
    }

    public static boolean await1(AFn ls, KDeferred x1) {
        if (x1.state() == 1) {
            return true;
        } else {
            x1.listen(new L0(ls));
            return false;
        }
    }

    public static boolean awaitIter(AFn ls, Iterator<?> ds) {
        KAwaiter ka = start(ls);
        while (ds.hasNext() && !ka.isDone()) {
            Object d = ds.next();
            if (d instanceof IDeferred) {
                KDeferred kd = KDeferred.wrapDeferred((IDeferred) d);
                ka.with(kd);
            }
        }
        return ka.await();
    }

    public static boolean awaitArr(AFn ls, KDeferred[] ds, int len) {
        KAwaiter ka = start(ls);
        int i = 0;
        for (; i+3 < len; i += 4) {
            ka.with(ds[i], ds[i+1], ds[i+2], ds[i+3]);
        }
        if (i+2 < len) {
            ka.with(ds[i], ds[i+1]);
        }
        if (i < len) {
            ka.with(ds[i]);
        }
        return ka.await();
    }
}