package ag.knitty;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Iterator;

public final class KaList<T> implements Iterable<T> {

  private static final int FROZEN = 0x100;
  private static final int MAX_SIZE = 256;
  private static final int SIZEMASK = MAX_SIZE - 1;

  static final class Iter<T> implements Iterator<T> {

    private KaList<T> node;
    private int idx;
    private int nsz;

    Iter(KaList<T> node) {
      this.node = node;
      this.idx = 0;
      this.nsz = node.freeze();
    }

    public boolean hasNext() {
      return idx < nsz;
    }

    @Override
    public T next() {

      Object res;
      while ((res = LSA.getVolatile(this.node.lss, idx)) == null);

      // advance idx
      if (++idx >= this.nsz) {
        this.idx = 0;
        this.node = this.node.next;
        this.nsz = this.node != null ? this.node.freeze() : -1;
      }

      return (T) res;
    }
  }

  private static final KaList<?> NOA = new KaList<>(0);
  private static final VarHandle POS;
  private static final VarHandle NEXT;
  private static final VarHandle LSA;

  static {
    try {
      MethodHandles.Lookup l = MethodHandles.lookup();
      POS = l.findVarHandle(KaList.class, "pos", int.class);
      NEXT = l.findVarHandle(KaList.class, "next", KaList.class);
      LSA = MethodHandles.arrayElementVarHandle(Object[].class);
    } catch (ReflectiveOperationException var1) {
      throw new ExceptionInInitializerError(var1);
    }
  }

  private final Object[] lss;
  private KaList<T> last = this;
  private volatile KaList<T> next = (KaList<T>) NOA;
  private volatile int pos;

  public KaList(int capacity) {
    this.lss = new Object[capacity];
  }

  public KaList() {
    this(8);
  }

  private static int growSize(int s) {
    return Math.min(MAX_SIZE, s * 2);
  }

  private KaList<T> pickNext() {
    KaList<T> next = this.next;
    if (next == NOA) {
      next = new KaList<T>(growSize(lss.length));
      KaList<T> curNext = (KaList<T>) NEXT.compareAndExchange(this, NOA, next);
      return curNext == NOA ? next : curNext;
    }
    return next;
  }

  int freeze() {
    boolean _x = NEXT.compareAndSet(this, NOA, (KaList<T>) null);
    return (int) POS.getAndBitwiseOr(this, FROZEN) & SIZEMASK;
  }

  private KaList<T> pushImpl(Object ls) {
    int pos;
    while ((pos = this.pos) < lss.length && !POS.compareAndSet(this, pos, pos + 1))
      ;
    if (pos < lss.length) {
      LSA.setVolatile(this.lss, pos, ls);
      return this;
    } else {
      KaList<T> n = this.pickNext();
      return n == null ? null : n.pushImpl(ls);
    }
  }

  public boolean push(T x) {
    if (x == null) {
      throw new IllegalArgumentException();
    }
    KaList<T> last = this.last;
    if (last != null) {
      return (this.last = last.pushImpl(x)) != null;
    } else {
      return false;
    }
  }

  public Iterator<T> iterator() {
    return new Iter<>(this);
  }
}
