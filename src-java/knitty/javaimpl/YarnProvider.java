package knitty.javaimpl;

import clojure.lang.Keyword;
import clojure.lang.AFn;

public interface YarnProvider {
    AFn yarn(Keyword k);
    AFn[] ycache();
}
