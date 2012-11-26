package com.senseidb.ba;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.senseidb.ba.gazelle.utils.OffHeapCompressedIntArray;




public class ByteBufferStorageTest {

  @Before
  public void setUp() throws Exception {
    }

  @Test
  public void test1() {
    OffHeapCompressedIntArray byteBufferStorage = new OffHeapCompressedIntArray(10000, 1);
    byte[] byteBuf = byteBufferStorage.getByteBuf();
    int[] values = new int[] {1,0,1};
    check(new OffHeapCompressedIntArray(10000, 10), new int[] {800,0,900});
    check(new OffHeapCompressedIntArray(10000, 20), new int[] {4000,0,900});
    check(byteBufferStorage, new int[] {1,0,1});
    check(new OffHeapCompressedIntArray(10000, 8), new int[] {255,0,255});
    
  }@Ignore
  @Test 
  public void test2() {
    OffHeapCompressedIntArray byteBufferStorage = new OffHeapCompressedIntArray(2000000, 10);
    byte[] byteBuf = byteBufferStorage.getByteBuf();
    
    for (int i = 0; i < 2000000; i++) {
      byteBufferStorage.addInt(i, i % 500, byteBuf);
    }
    int[] arr = new int[2000000];
    Arrays.fill(arr, 1000);
    while (true) {
      int sum = 0;
      long time = System.currentTimeMillis();
      for (int i = 0; i < 2000000; i++) {
        sum += byteBufferStorage.getInt(i);
      }
      System.out.println("Time = " + (System.currentTimeMillis() - time) + " sum = " + sum);
    } 
    
  }
  private void check(OffHeapCompressedIntArray byteBufferStorage, int[] values) {
    byte[] byteBuf = byteBufferStorage.getByteBuf();
   for (int i = 0; i < values.length; i++) {
    byteBufferStorage.addInt(i, values[i], byteBuf);
   }
   for (int i = 0; i < values.length; i++) {
     assertEquals(values[i], byteBufferStorage.getInt(i));
   }
  }

}
