package knitty.javaimpl;

import clojure.lang.AFn;
import clojure.lang.IFn;
import manifold.deferred.IDeferredListener;

public abstract class AListener implements IDeferredListener {

    public Object onSuccess(Object x) { return null; }

    public Object onError(Object e) { return null; }

    public static AListener fromFn(Object onVal, Object onErr) {
        return new Fn(asAFn(onVal), asAFn(onErr));
    }

    private static AFn asAFn(Object f) {
        if (f instanceof AFn) {
            return (AFn) f;
        } else {
            IFn ff = (IFn) f;
            return new AFn() {
                public Object invoke(Object x) {
                    return ff.invoke(x);
                }
            };
        }
    }

    private static final class Fn extends AListener {

        private final AFn onSucc;
        private final AFn onErr;

        Fn(AFn onSucc, AFn onErr) {
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
