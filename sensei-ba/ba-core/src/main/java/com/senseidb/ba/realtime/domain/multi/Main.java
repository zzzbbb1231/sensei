package com.senseidb.ba.realtime.domain.multi;



public class Main {
public static void main(String[] args) {
  com.senseidb.ba.gazelle.utils.HeapCompressedIntArray array = new com.senseidb.ba.gazelle.utils.HeapCompressedIntArray(10000, 20);
  for (int i = 0; i < 10000; i++) {
    array.setInt(i, 1);
  }
  for (int i = 0; i < 100; i++) {
    System.out.println(array.getInt(i));
  }
}
}
