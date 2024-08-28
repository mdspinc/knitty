package knitty.javaimpl;

import clojure.lang.AFn;
import clojure.lang.Associative;
import clojure.lang.IKVReduce;
import clojure.lang.ILookup;
import clojure.lang.IReduceInit;
import clojure.lang.Keyword;

abstract class YankInputs extends AFn implements IKVReduce, IReduceInit, ILookup {
    public abstract Object get(int i, Keyword k, Object fallback);
    public abstract Associative toAssociative();
}
