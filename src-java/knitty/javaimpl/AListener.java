package knitty.javaimpl;

import clojure.lang.IFn;
import manifold.deferred.IDeferredListener;

public abstract class AListener implements IDeferredListener {

    public Object onSuccess(Object x) { return null; }

    public Object onError(Object e) { return null; }

    public static AListener fromFn(Object onVal, Object onErr) {
        return new Fn((IFn) onVal, (IFn) onErr);
    }

    private static final class Fn extends AListener {

        private final IFn onSucc;
        private final IFn onErr;

        Fn(IFn onSucc, IFn onErr) {
            this.onSucc = onSucc;
            this.onErr = onErr;
        }

        public Object onSuccess(Object x) {
            this.onSucc.invoke(x);
            return null;
        }

        public Object onError(Object e) {
            this.onErr.invoke(e);
            return null;
        }
    }
}
