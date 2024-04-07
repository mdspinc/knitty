package knitty.javaimpl;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import clojure.lang.Keyword;

public final class KwMapper {

    private static int INIT_SIZE = 64;  // pow of 2!
    private static final Object LOCK = new Object();

    private static Keyword[] ksa = new Keyword[INIT_SIZE];
    private static final Map<Keyword, Integer> ksm = new ConcurrentHashMap<>(INIT_SIZE);

    private static int kid;
    private static int[] ksar = new int[INIT_SIZE];
    private static int ncol;

    public static int maxi() {
        return kid + 1;
    }

    public static int reg(Keyword k) {
        Integer v = ksm.get(k);
        if (v != null) {
            return v.intValue();
        } else {
            synchronized (LOCK) {
                int t = ++kid;
                ksm.put(k, t);
                if (t >= ksa.length) {
                    ksa = Arrays.copyOf(ksa, ksa.length * 2);
                }
                ksa[t] = k;
                return t;
            }
        }
    }

    public static Keyword get(int i) {
        return ksa[i];
    }

    public static int getr(Keyword k, boolean cache) {

        int m = ksar.length - 1;
        int h = k.hasheq();
        h = (h ^ (h >>> 16)) & m;

        for (int i = ncol; i >= 0; i--) {
            int t = ksar[h];
            if (ksa[t] == k) {
                return t;
            }
            if (t == 0) {
                int r = ksm.getOrDefault(k, -1).intValue();
                if (cache && r != -1) ksar[h] = r;
                return r;
            }
            h = (h + 1) & m;
        }

        if (cache && ncol < 8) {
            // dont put items when >8 collisions, just fallback to map
            invalidateKsar();
        }

        return ksm.getOrDefault(k, -1).intValue();
    }

    private static void invalidateKsar() {
        synchronized (LOCK) {
            int ncap = Integer.highestOneBit(ksar.length) * 2;
            int maxcap = Integer.highestOneBit(ksm.size() * 32);
            int maxcol = Integer.max(0, (int) (Math.log((float) ksar.length / maxcap) + Math.E));

            if (ncol < maxcol || ncap >= maxcap) {
                ncol++;
            } else {
                ksar = new int[ncap];
            }
        }
    }
}
