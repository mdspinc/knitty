package knitty.javaimpl;

import java.util.concurrent.atomic.AtomicReference;

import clojure.lang.Associative;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;

public final class KwMapper {

    public static final AtomicReference<KwMapper> INSTANCE = new AtomicReference<>(new KwMapper());

    private final int maxId;
    private final Associative keyword2Id;
    private final Associative id2Keyword;

    private final Keyword[] id2KeywordCache;
    private final int[] keywordHash2IdCache;
    private final int hashMask;
    private final int maxCollisions;

    static final int roundUpBits(int x, int bits) {
        int mask = (1 << bits) - 1;
        return (x | mask) + 1;
    }

    KwMapper(
        int maxId,
        Associative keyword2Id,
        Associative id2Keyword,
        Keyword[] id2KeywordCache,
        int[] keywordHarh2IdCache
    ) {
        this.maxId = maxId;
        this.keyword2Id = keyword2Id;
        this.id2Keyword = id2Keyword;

        int hashCacheLen = roundUpBits(maxId * 8, 12);
        this.hashMask = hashCacheLen - 1;

        this.keywordHash2IdCache = (
            (keywordHarh2IdCache != null && keywordHarh2IdCache.length == hashCacheLen)
            ? keywordHarh2IdCache
            : new int[hashCacheLen]
        );
        this.id2KeywordCache = (
            (id2KeywordCache != null && id2KeywordCache.length < maxId)
            ? id2KeywordCache
            : new Keyword[roundUpBits(maxId, 10)]
        );

        this.maxCollisions = 8;
    }

    KwMapper(int maxId, Associative keyword2Id, Associative id2Keyword) {
        this(maxId, keyword2Id, id2Keyword, null, null);
    }

    KwMapper() {
        this(0, PersistentHashMap.EMPTY, PersistentHashMap.EMPTY);
    }

    public int maxIndex() {
        return maxId;
    }

    public KwMapper addKeyword(Keyword k) {
        if (keyword2Id.containsKey(k)) {
            return this;
        }
        int t = this.maxId + 1;
        return new KwMapper(
            t,
            keyword2Id.assoc(k, t),
            id2Keyword.assoc(t, k),
            id2KeywordCache,
            keywordHash2IdCache);
    }

    public Keyword resolveByIndex(int i) {
        Keyword r = this.id2KeywordCache[i];
        if (r == null) {
            r = (Keyword) id2Keyword.valAt(i);
            this.id2KeywordCache[i] = r;
        }
        return r;
    }

    public int resolveByKeyword(Keyword k) {
        int h = k.hasheq() & hashMask;
        int t = keywordHash2IdCache[h];
        if (t != 0 && k == this.id2KeywordCache[t]) {
            return t;
        }
        return resolveByKeyword0(h, k);
    }

    private int resolveByKeyword0(int h, Keyword k) {
        for (int i = 0; i < maxCollisions; i++) {
            h = (h + i) & hashMask;
            int t = keywordHash2IdCache[h];
            if (t == 0) {
                int r = (int) keyword2Id.valAt(k, -1);
                if (r != -1) keywordHash2IdCache[h] = r;
                return r;
            } else if (resolveByIndex(t) == k) {
                return t;
            }
        }
        return (int) keyword2Id.valAt(k, -1);
    }

    public static final int registerKeyword(Keyword k) {
        KwMapper km = INSTANCE.updateAndGet(kw -> kw.addKeyword(k));
        return km.resolveByKeyword(k);
    }

    public static KwMapper getInstance() {
        return INSTANCE.get();
    }
}
