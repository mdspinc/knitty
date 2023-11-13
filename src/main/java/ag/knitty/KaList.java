package ag.knitty;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Iterator;

public final class KaList<T> implements Iterable<T> {

  private static final int FINISHD = 0x800;
  private static final int HASNEXT = 0x400;
  private static final int MAX_SIZE = 0x080;
  private static final int SIZEMASK = MAX_SIZE - 1;
  private static final int FLGSMASK = FINISHD | HASNEXT;

  static final class Iter<T> implements Iterator<T> {

    private int nsz;
    private int idx;
    private KaList<T> node;

    Iter(KaList<T> node) {
      this.nsz = node.freeze();
      this.node = node;
    }

    public boolean hasNext() {
      return idx < nsz;
    }

    public T next() {

      Object res;
      while ((res = LSA.getVolatile(this.node.lss, idx)) == null)
        Thread.onSpinWait();

      // advance idx
      if (++idx >= this.nsz) {
        this.idx = 0;
        this.node = this.node.pickNext();
        this.nsz = this.node != null ? this.node.freeze() : -1;
      }

      return (T) res;
    }
  }

  static final VarHandle POS;
  static final VarHandle LSA;

  static {
    try {
      MethodHandles.Lookup l = MethodHandles.lookup();
      POS = l.findVarHandle(KaList.class, "pos", int.class);
      LSA = MethodHandles.arrayElementVarHandle(Object[].class);
    } catch (ReflectiveOperationException var1) {
      throw new ExceptionInInitializerError(var1);
    }
  }

  final Object[] lss;
  private KaList<T> last = this;
  private KaList<T> next;
  private int pos;

  public KaList(int capacity) {
    this.lss = new Object[capacity];
  }

  public KaList() {
    this(8);
  }

  private static int growSize(int s) {
    return Math.min(MAX_SIZE, s * 2);
  }

  KaList<T> pickNext() {
    int pos = this.pos;

    if ((pos & FINISHD) == 0) {
      pos = (int) POS.getAndBitwiseOr(this, FINISHD);
      if ((pos & FLGSMASK) == 0) {
        this.next = new KaList<T>(growSize(lss.length));
        pos = (int) POS.getAndBitwiseOr(this, HASNEXT);
      }
    }

    while ((pos & HASNEXT) == 0) {
      Thread.onSpinWait();
      pos = this.pos;
    }

    return this.next;
  }

  int freeze() {
    int pos = (int) POS.getAndBitwiseOr(this, FINISHD);
    if ((pos & FLGSMASK) == 0) {
      pos = (int) POS.getAndBitwiseOr(this, HASNEXT);
    }
    return pos & SIZEMASK;
  }

  private KaList<T> pushImpl(Object ls) {
    int pos = this.pos;
    while (pos < lss.length && !POS.compareAndSet(this, pos, pos + 1)) {
      pos = this.pos;
    }
    if (pos < lss.length) {
      LSA.setVolatile(this.lss, pos, ls);
      return this;
    }
    KaList<T> next = this.pickNext();
    return next != null ? next.pushImpl(ls) : null;
  }

  public boolean push(T x) {
    if (x == null) {
      throw new IllegalArgumentException();
    }
    KaList<T> last = this.last;
    if (last != null) {
      KaList<T> newLast = last.pushImpl(x);
      if (last != newLast) {
        this.last = newLast;
      }
      return newLast != null;
    } else {
      return false;
    }
  }

  public Iterator<T> iterator() {
    return new Iter<>(this);
  }
}
