package knitty.javaimpl;

import clojure.lang.IFn;
import manifold.deferred.IDeferredListener;

public abstract class AListener {

    AListener next;

    public abstract void success(Object x);
    public abstract void error(Object e);

    public static AListener fromFn(Object onVal, Object onErr) {
        return new Fn((IFn) onVal, (IFn) onErr);
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
    }

    static final class Fn extends AListener {

        private final IFn onSucc;
        private final IFn onErr;

        Fn(IFn onSucc, IFn onErr) {
            this.onSucc = onSucc;
            this.onErr = onErr;
        }

        public void success(Object x) {
            this.onSucc.invoke(x);
        }

        public void error(Object e) {
            this.onErr.invoke(e);
        }
    }
}
