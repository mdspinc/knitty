package knitty.javaimpl;

import clojure.lang.Keyword;
import clojure.lang.IFn;

public interface YarnProvider {
    IFn yarn(Keyword k);
    IFn[] ycache();
}
