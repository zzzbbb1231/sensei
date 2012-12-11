package com.senseidb.search.req.mapred;

public class DefaultIntArray implements IntArray {
  private final int[] arr;

  public DefaultIntArray(int[] arr) {
    this.arr = arr;
  }

  @Override
  public int size() {
    return arr.length;
  }

  @Override
  public int get(int index) {
    return arr[index];
  }

}
