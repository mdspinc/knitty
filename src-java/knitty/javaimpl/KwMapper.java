package knitty.javaimpl;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;

import clojure.lang.Keyword;

public final class KwMapper {

    private static int INIT_SIZE = 4;
    private static KwMapper INSTANCE = new KwMapper();

    public static KwMapper getIntance() {
        return INSTANCE;
    }

    public static void resetKeywordsPoolForTests() {
        INSTANCE = new KwMapper();
    }

    public static int regkw(Keyword kw) {
        return INSTANCE.reg(kw);
    }

    public static int maxi() {
        return INSTANCE.kid + 1;
    }

    private Keyword[] ksa = new Keyword[INIT_SIZE];
    private final Map<Keyword, Integer> ksm = new ConcurrentHashMap<>(INIT_SIZE);

    private int kid;
    private int[] ksar = new int[INIT_SIZE];
    private int ncol;

    public int reg(Keyword k) {
        Integer v = ksm.get(k);
        if (v != null) {
            return v.intValue();
        } else {
            synchronized (this) {
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

    public Keyword get(int i) {
        return ksa[i];
    }

    private int getr0(Keyword k) {
        Integer r = ksm.get(k);
        if (r == null) {
            throw new IllegalArgumentException("unknown yarn " + k);
        }
        return r.intValue();
    }

    public int getr(Keyword k) {

        if (ncol > 8) {
            return getr0(k);
        }

        int h = System.identityHashCode(k) & (ksar.length - 1);
        for (int i = ncol; i >= 0; i--) {
            int t = ksar[h];
            if (ksa[t] == k) {
                return t;
            }
            if (t == 0) {
                return (ksar[h] = getr0(k));
            }
            h = h < ksar.length ? h + 1 : 0;
        }

        invalidateKsar();
        return getr0(k);
    }

    private synchronized void invalidateKsar() {

        int ncap = Integer.highestOneBit(ksar.length) * 2;
        int maxcap = Integer.highestOneBit(ksm.size() * 32);
        int maxcol = Integer.max(0, (int) Math.log((float) ksar.length / maxcap) + 3);

        if (ncol < maxcol || ncap >= maxcap) {
            ncol++;
        } else {
            this.ksar = new int[ncap];
        }
    }
}
