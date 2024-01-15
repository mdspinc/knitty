package knitty.javaimpl;

import java.util.Collection;
import clojure.lang.Keyword;

public interface Yarn {
    void yank(YankCtx mdm, KDeferred d);
    Keyword key();
    Collection<Keyword> deps();
}
