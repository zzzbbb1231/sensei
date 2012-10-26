package com.senseidb.ba.gazelle.readers;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedInts.Mutable;

import com.senseidb.ba.gazelle.utils.CompressedIntArray;

public class CompressionTest {
  public static void main(String[] args) {
    int size = 10000000;
    int dictSize = 100;
    int numOfLookups = size;
    int[] arr = new int[numOfLookups];
    
    for (int i = 0; i < numOfLookups; i++) {
      arr[i] = i ;
    }
    shuffleArray(arr);
    Mutable mutable = PackedInts.getMutable(size, CompressedIntArray.getNumOfBits(dictSize)); 
    CompressedIntArray intArray = new CompressedIntArray(size, CompressedIntArray.getNumOfBits(dictSize));
    ByteBuffer byteBuffer =  ByteBuffer.allocateDirect(size);
    for (int i = 0; i < size; i++) {
      mutable.set(i, i % dictSize );
      intArray.addInt(i, i % dictSize);
    }
    long time = 0;
    long count = 0;
    while (true) {
      time = System.currentTimeMillis();
       count = 0;
      for (int i = 0; i < size; i++) {
        //count += mutable.get(arr[i]);
        count += mutable.get(i);
      }
      System.out.println("PackedInt Time = " + (System.currentTimeMillis() - time) + ", count = " + count);
      time = System.currentTimeMillis();
      count = 0;
     for (int i = 0; i < size; i++) {
       //count += intArray.readInt(arr[i]);
       count += intArray.readInt(i);
     }
     System.out.println("CompressedInt Time = " + (System.currentTimeMillis() - time) + ", count = " + count);
    
    }
  }
  public static void shuffleArray(int[] a) {
    int n = a.length;
    Random random = new Random();
    random.nextInt();
    for (int i = 0; i < n; i++) {
      int change = i + random.nextInt(n - i);
      int tmp = change;
      a[change] = i;
      a[i] = tmp;
    }
  }
}
