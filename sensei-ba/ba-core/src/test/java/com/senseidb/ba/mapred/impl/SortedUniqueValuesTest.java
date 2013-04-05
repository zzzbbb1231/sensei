package com.senseidb.ba.mapred.impl;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

public class SortedUniqueValuesTest {

  @Test
  public void test1Trim() {
    long[] arr = new long[] {10,12,2,3,5,7,7};
    SortedUniqueValues.trim(arr, arr.length, 5);    
    assertArrayEquals(new long[] {10,12,5,7,7,7,7}, arr);
  }
  @Test
  public void test2Merge() {
    List<MapResult> combine = new SortedUniqueValues().combine(java.util.Arrays.asList(new MapResult(new long[]{pair(1,2), pair(2,3), pair(3,4)}, 3), new MapResult(new long[]{pair(1,3), pair(2,2), pair(3,3)}, 3), new MapResult(new long[]{pair(0,3),pair(1,1), pair(2,1), pair(3,1)}, 4)), null);
    MapResult mapResult = combine.get(0);
    for (int i = 0; i < combine.get(0).count; i++) {
      System.out.println(unpair(combine.get(0).vals[i]));
    }
    assertEquals(getMemberId(mapResult.vals[0] ), 0);
    assertEquals(getTime(mapResult.vals[0]), 3);
    assertEquals(getMemberId(mapResult.vals[1] ), 1);
    assertEquals(getTime(mapResult.vals[1]), 3);
    assertEquals(getMemberId(mapResult.vals[2] ), 2);
    assertEquals(getTime(mapResult.vals[2]), 3);
    assertEquals(getMemberId(mapResult.vals[3]), 3);
    assertEquals(getTime(mapResult.vals[3]), 4);
  }
  @Test
  public void test3Render() {
    List<MapResult> combine = new SortedUniqueValues().combine(java.util.Arrays.asList(new MapResult(new long[]{pair(1,2), pair(2,3), pair(3,4)}, 3), new MapResult(new long[]{pair(1,3), pair(2,2), pair(3,3)}, 3), new MapResult(new long[]{pair(0,3),pair(1,1), pair(2,1), pair(3,1)}, 4)), null);
    MapResult mapResult = combine.get(0);
    System.out.println(new SortedUniqueValues().render(mapResult));
   
  }

  public static long pair(int p1, int p2) {
    long ret = p1;
    ret <<= 32;
    return ret | p2; 
  }
  public static long getTime(long val) {
    return val & Integer.MAX_VALUE;
  }
  public static long getMemberId(long val) {
    return val  >>> 32;
  }
  public static String unpair(long number) {
    return "first = " + (number >>> 32) + ", second = " + (number & Integer.MAX_VALUE);
  }
}
