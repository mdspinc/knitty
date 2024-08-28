package knitty.javaimpl;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import clojure.lang.Associative;
import clojure.lang.IFn;
import clojure.lang.IKVReduce;
import clojure.lang.IReduceInit;
import clojure.lang.Keyword;
import clojure.lang.RT;

final class YankInputsAssoc extends YankInputs {

    private final Associative wrapped;

    public YankInputsAssoc(clojure.lang.Associative wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Object kvreduce(IFn f, Object a) {
        if (wrapped instanceof IKVReduce) {
            IKVReduce r = (IKVReduce) wrapped;
            return r.kvreduce(f, a);
        } else {
            @SuppressWarnings("unchecked")
            Iterator<Map.Entry<Object, Object>> it = (Iterator<Entry<Object, Object>>) RT.iter(wrapped);
            while (it.hasNext()) {
                Map.Entry<?,?> e = it.next();
                a = f.invoke(a, e.getKey(), e.getValue());
            }
            return a;
        }
    }

    @Override
    public Object reduce(IFn f, Object a) {
        if (wrapped instanceof IReduceInit) {
            IReduceInit r = (IReduceInit) wrapped;
            return r.reduce(f, a);
        } else {
            @SuppressWarnings("unchecked")
            Iterator<Map.Entry<Object, Object>> it = (Iterator<Entry<Object, Object>>) RT.iter(wrapped);
            while (it.hasNext()) {
                a = f.invoke(a, it.next());
            }
            return a;
        }
    }

    @Override
    public Object get(int i, Keyword k, Object fallback) {
        return wrapped.valAt(k, fallback);
    }

    @Override
    public Object valAt(Object k, Object fallback) {
        return wrapped.valAt(k, fallback);
    }

    @Override
    public Associative toAssociative() {
        return wrapped;
    }

    @Override
    public Object valAt(Object key) {
        return valAt(key, null);
    }
}
