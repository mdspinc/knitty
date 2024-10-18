package knitty.javaimpl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

import clojure.lang.IFn;
import manifold.deferred.IDeferredListener;

abstract class AListener {

    private final static VarHandle NEXT;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            NEXT = l.findVarHandle(AListener.class, "next", AListener.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public AListener next;

    public abstract void success(Object x);
    public abstract void error(Object e);

    public final boolean casNext(AListener curNext, AListener newNext) {
        return NEXT.compareAndSet(this, curNext, newNext);
    }

    protected AListener() {
    }

    static final class Dl extends AListener {

        static final byte ACTIVE = 0;
        static final byte PENDING_CANCEL = 1;
        static final byte CANCELLED = 2;

        static final VarHandle CANCEL;
        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                CANCEL = l.findVarHandle(AListener.Dl.class, "cancel", Byte.TYPE);
            } catch (ReflectiveOperationException var1) {
                throw new ExceptionInInitializerError(var1);
            }
        }

        IDeferredListener ls;
        byte cancel;

        public Dl(IDeferredListener ls) {
            this.ls = ls;
        }

        @Override
        public void success(Object x) {
            if (((byte) CANCEL.getOpaque(this)) == PENDING_CANCEL) {
                CANCEL.setOpaque(this, CANCELLED);
                this.ls = null;
            } else {
                this.ls.onSuccess(x);
            }
        }

        @Override
        public void error(Object e) {
            if (((byte) CANCEL.getOpaque(this)) == PENDING_CANCEL) {
                CANCEL.setOpaque(this, CANCELLED);
                this.ls = null;
            } else {
                this.ls.onError(e);
            }
        }

        public boolean cancelled() {
            return ((byte) CANCEL.getOpaque(this)) == CANCELLED;
        }

        public boolean acquireForCancel() {
            return CANCEL.compareAndSet(this, ACTIVE, PENDING_CANCEL);
        }

        @Override
        public String toString() {
            return super.toString() + "[ls=" + Objects.toString(ls) + "]";
        }
    }

    static final class Fn extends AListener {

        private final IFn onSucc;
        private final IFn onErr;

        public Fn(IFn onSucc, IFn onErr) {
            this.onSucc = onSucc;
            this.onErr = onErr;
        }

        @Override
        public void success(Object x) {
            if (onSucc != null) {
                this.onSucc.invoke(x);
            }
        }

        @Override
        public void error(Object e) {
            if (onErr != null) {
                this.onErr.invoke(e);
            }
        }

        @Override
        public String toString() {
            return super.toString() + "[onSucc=" + Objects.toString(onSucc) + ", onErr=" + Objects.toString(onErr) + "]";
        }
    }
}
