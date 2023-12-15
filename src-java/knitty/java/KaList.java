package knitty.java;

import java.util.Iterator;

public final class KaList<T> implements Iterable<T> {

  private static final int MAX_SIZE = 256;

  static final class Node {

    final Object[] items;
    Node next;

    public Node(int size) {
      this.items = new Object[size];
    }
  }

  static final class Iter<T> implements Iterator<T> {

    private Node node;
    private int idx;
    private int len;
    private int lpos;

    Iter(int lpos, Node node) {
      this.node = node;
      this.lpos = lpos;
      if (node != null) this.len = node.next == null ? lpos : node.items.length;
    }

    public boolean hasNext() {
      return node != null ;
    }

    @SuppressWarnings("unchecked")
    public T next() {
      Object res = node.items[idx++];
      if (idx == len) {
        this.node = node.next;
        if (this.node != null) {
          this.idx = 0;
          this.len = this.node.next == null ? lpos : this.node.items.length;
        }
      }
      return (T) res;
    }
  }

  private Node first;
  private Node last;
  private final int initsize;
  private int pos;
  private boolean frozen;

  public KaList(int capacity) {
    this.initsize = capacity;
  }

  public KaList() {
    this(8);
  }

  public synchronized boolean push(T x) {
    if (frozen) {
      return false;
    }
    if (this.first == null) {
      this.first = this.last = new Node(this.initsize);
    } else if (this.pos == this.last.items.length) {
      Node n = new Node(Math.min(MAX_SIZE, this.last.items.length * 2));
      this.last.next = n;
      this.last = n;
      this.pos = 0;
    }
    this.last.items[this.pos++] = x;
    return true;
  }

  public synchronized Iterator<T> iterator() {
    this.frozen = true;
    return new Iter<>(pos, first);
  }

  public synchronized void clean() {
    this.first = null;
    this.last = null;
    this.frozen = true;
  }
}
