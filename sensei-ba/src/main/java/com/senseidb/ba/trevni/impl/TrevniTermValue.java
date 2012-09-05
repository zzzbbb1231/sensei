package com.senseidb.ba.trevni.impl;

public class TrevniTermValue<T> implements Comparable<T> {

  private T _val;
  
  public TrevniTermValue(T val) {
    _val = val;
  }

  @Override
  public int compareTo(T o) {
    String val = (String)_val;
    return val.compareTo((String)o);
  }

  public T get() {
    return _val;
  }
}
