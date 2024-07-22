package knitty.javaimpl;

import java.util.Objects;

import clojure.lang.IFn;
import clojure.lang.Var;
import manifold.deferred.IDeferredListener;

abstract class AListener {

    AListener next;
    final Object frame;

    public abstract void success(Object x);
    public abstract void error(Object e);

    AListener() {
        this.frame = Var.cloneThreadBindingFrame();
    }

    protected final void resetFrame() {
        Var.resetThreadBindingFrame(frame);
    }

    static final class Dl extends AListener {

        private final IDeferredListener ls;

        Dl(IDeferredListener ls) {
            this.ls = ls;
        }

        public void success(Object x) {
            this.ls.onSuccess(x);
        }

        public void error(Object e) {
            this.ls.onError(e);
        }

        @Override
        public String toString() {
            return super.toString() + "[ls=" + Objects.toString(ls) + "]";
        }
    }

    static final class Fn extends AListener {

        private final IFn onSucc;
        private final IFn onErr;

        Fn(IFn onSucc, IFn onErr) {
            this.onSucc = onSucc;
            this.onErr = onErr;
        }

        public void success(Object x) {
            if (onSucc != null) {
                this.onSucc.invoke(x);
            }
        }

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
