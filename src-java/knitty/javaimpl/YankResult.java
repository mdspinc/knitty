package knitty.javaimpl;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import clojure.lang.AFn;
import clojure.lang.Associative;
import clojure.lang.Delay;
import clojure.lang.IDeref;
import clojure.lang.IEditableCollection;
import clojure.lang.IFn;
import clojure.lang.IKVReduce;
import clojure.lang.ILookup;
import clojure.lang.IMapEntry;
import clojure.lang.IMeta;
import clojure.lang.IObj;
import clojure.lang.IPersistentMap;
import clojure.lang.IReduceInit;
import clojure.lang.ISeq;
import clojure.lang.ITransientAssociative;
import clojure.lang.Keyword;
import clojure.lang.RT;
import clojure.lang.Seqable;
import knitty.javaimpl.YankCtx.KVCons;

public final class YankResult extends AFn implements Iterable<Object>, ILookup, Seqable, IObj, IKVReduce, IReduceInit, IDeref {

    private static final int ASHIFT = YankCtx.ASHIFT;
    private static final int AMASK = YankCtx.AMASK;

    final Associative inputs;
    final KDeferred[][] yrns;
    final YankCtx.KVCons added;
    final KwMapper kwmapper;
    final IPersistentMap meta;

    private final Delay mapDelay = new Delay(new AFn() {
        @Override
        public Object invoke() {
            return toMap0();
        }
    });

    protected YankResult(Associative inputs, KDeferred[][] yrns, YankCtx.KVCons added, KwMapper kwmapper) {
        this.inputs = inputs;
        this.yrns = yrns;
        this.added = added;
        this.kwmapper = kwmapper;
        this.meta = (inputs instanceof IMeta) ? ((IMeta) inputs).meta() : null;
    }

    private YankResult(Associative inputs, KDeferred[][] yrns, YankCtx.KVCons added, KwMapper kwmapper, IPersistentMap meta) {
        this.inputs = inputs;
        this.yrns = yrns;
        this.added = added;
        this.kwmapper = kwmapper;
        this.meta = meta;
    }

    Object toMap0() {
        KVCons added0 = added;
        if (added0.next == null) {
            return inputs;
        }
        Object result;
        if (added0.next.d != null && inputs instanceof IEditableCollection) {
            ITransientAssociative t = (ITransientAssociative) ((IEditableCollection) inputs).asTransient();
            for (KVCons a = added0; a.d != null; a = a.next) {
                t = t.assoc(a.k, a.d.unwrap());
            }
            result = t.persistent();
        } else {
            Associative t = inputs;
            for (KVCons a = added0; a.d != null; a = a.next) {
                t = t.assoc(a.k, a.d.unwrap());
            }
            result = t;
        }
        if (result instanceof IObj) {
            result = ((IObj) result).withMeta(meta);
        }
        return result;
    }

    public IPersistentMap toMap() {
        return (IPersistentMap) mapDelay.deref();
    }

    @Override
    public Object deref() {
        return mapDelay.deref();
    }

    private KDeferred pull(Object key) {
        if (key instanceof Keyword) {
            int i = kwmapper.resolveByKeyword((Keyword) key, false);
            if (i == 0) {
                return null;
            }
            int i0 = i >> ASHIFT;
            int i1 = i & AMASK;
            KDeferred[] yrns1 = yrns[i0];
            if (yrns1 == null) {
                return null;
            }
            return yrns1[i1];
        }
        return null;
    }

    @Override
    public Iterator<Object> iterator() {
        return new Iterator<Object>() {

            final Iterator<?> insIter = (Iterator<?>) RT.iter(inputs);
            volatile YankCtx.KVCons kvcons = added;

            public boolean hasNext() {
                return kvcons.next != null || insIter.hasNext();
            }

            public Object next() {
                if (kvcons.next != null) {
                    IMapEntry e = kvcons;
                    kvcons = kvcons.next;
                    return e;
                }
                return insIter.next();
            }
        };
    }

    @Override
    public ISeq seq() {
        return RT.chunkIteratorSeq(iterator());
    }

    @Override
    public Object valAt(Object key) {
        KDeferred d = pull(key);
        return d != null ? d.unwrap() : inputs.valAt(key);
    }

    @Override
    public Object valAt(Object key, Object notFound) {
        KDeferred d = pull(key);
        return d != null ? d.unwrap() : inputs.valAt(key, notFound);
    }

    @Override
    public IPersistentMap meta() {
        return meta;
    }

    @Override
    public IObj withMeta(IPersistentMap meta) {
        return new YankResult(inputs, yrns, added, kwmapper, meta);
    }

    @Override
    public Object kvreduce(IFn f, Object a) {
        if (inputs instanceof IKVReduce) {
            IKVReduce r = (IKVReduce) inputs;
            a = r.kvreduce(f, a);
        } else {
            @SuppressWarnings("unchecked")
            Iterator<Map.Entry<Object, Object>> it = (Iterator<Entry<Object, Object>>) RT.iter(inputs);
            while (it.hasNext()) {
                Map.Entry<?,?> e = it.next();
                a = f.invoke(a, e.getKey(), e.getValue());
            }
        }
        for (KVCons x = added; x.d != null; x = x.next) {
            a = f.invoke(a, x.k, x.d.unwrap());
        }
        return a;
    }

    @Override
    public Object reduce(IFn f, Object a) {
        if (inputs instanceof IReduceInit) {
            IReduceInit r = (IReduceInit) inputs;
            a = r.reduce(f, a);
        } else {
            @SuppressWarnings("unchecked")
            Iterator<Map.Entry<Object, Object>> it = (Iterator<Entry<Object, Object>>) RT.iter(inputs);
            while (it.hasNext()) {
                a = f.invoke(a, it.next());
            }
        }
        for (KVCons x = added; x.d != null; x = x.next) {
            a = f.invoke(a, x);
        }
        return a;
    }

    @Override
    public Object invoke() {
	    return mapDelay.deref();
    }

    @Override
    public Object invoke(Object arg1) {
	    return valAt(arg1);
    }

    @Override
    public Object invoke(Object arg1, Object arg2) {
	    return valAt(arg1, arg2);
    }
}
