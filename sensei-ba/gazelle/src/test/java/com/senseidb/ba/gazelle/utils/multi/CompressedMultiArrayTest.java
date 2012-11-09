package com.senseidb.ba.gazelle.utils.multi;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.browseengine.bobo.util.BigIntArray;
import com.senseidb.ba.gazelle.utils.ReadMode;

public class CompressedMultiArrayTest {

  private CompressedMultiArray compressedMultiArray;
  private int[] buffer;
  
  @Before
  public void setUp() throws Exception {
    compressedMultiArray = new CompressedMultiArray(10, 1000);
    compressedMultiArray.setMaxNumOfElementsPerChunk(909);
     for (int i = 0 ; i < 10000; i ++) {
       int length = i % 10;
       int[] arr = new int[length];
       Arrays.fill(arr, i % 10);
       compressedMultiArray.add(arr);
     }
     compressedMultiArray.initSkipLists(); 
     buffer = new int[10];
  }

  @After
  public void tearDown() throws Exception {
  }
  
  @Test
  public void testValidateRangeFindMethod() {    
    MultiFacetIterator iterator = compressedMultiArray.iterator();
    int index = 0;
    int i =0;
    boolean response = true;
    while (true) {
      index = iterator.find(index + 1, 5, 7);
      if (index == -1) {
        break;
      }
      if ((index%10 <5) || (index%10 >7)) {
        response = false;
      }
      i++;
    }
    assertTrue(response);
    assertEquals(3000, i);
    
    assertEquals(9, compressedMultiArray.getMaxNumValuesPerDoc());
  }
  
  @Test
  public void test3ReadAllValues() {    
    MultiFacetIterator iterator = compressedMultiArray.iterator();
    
    for (int i = 0; i < 10000; i++) {
    
     assertTrue(iterator.advance(i));
      int length = i % 10;
      assertEquals(length, iterator.readValues(buffer));
      for (int j = 0; j < length; j++) {
        assertEquals(length, buffer[j]);
      }
    }
    assertEquals(9, compressedMultiArray.getMaxNumValuesPerDoc());
  }
  
  
  
  @Test
  public void test7FindValues() {    
    MultiFacetIterator iterator = compressedMultiArray.iterator();
    int index = 0;
    int i =0;
    
    while (true) {
      index = iterator.find(index + 1, 9);
      if (index == -1) {
        break;
      }
      assertEquals(9, index % 10);
      i++;
    }
    assertEquals(1000, i);
    
    assertEquals(9, compressedMultiArray.getMaxNumValuesPerDoc());
  }
  @Test
  public void test4ReadAllValuesWith1500Increment() {    
    MultiFacetIterator iterator = compressedMultiArray.iterator();    
    for (int i = 0; i < 10000; i+=500) {     
      assertTrue(iterator.advance(i));
      int length = i % 10;
      assertEquals(length, iterator.readValues(buffer));
      for (int j = 0; j < length; j++) {
        assertEquals(length, buffer[j]);
      }
    }
  }
  @Test
  public void test2ReadNonExistingValue() {    
    MultiFacetIterator iterator = compressedMultiArray.iterator();
    assertFalse(iterator.advance(10001));  
  }
  
  @Test
  public void test6ReadAllValuesWith1000IncrementAfterPersistingAndReadingThefile() throws Exception {    
    File dir = new File("temp");
    try {
    FileUtils.deleteDirectory(dir);
    dir.mkdirs();
   
    compressedMultiArray.flushToFile(dir, "multiValueColumnName");
    compressedMultiArray = CompressedMultiArray.readFromFile( dir, "multiValueColumnName", 10, ReadMode.DirectMemory);
   
    MultiFacetIterator iterator = compressedMultiArray.iterator();
    
    for (int i = 0; i < 10000; i+=1) {
     
      assertTrue(iterator.advance(i));
      int length = i % 10;      
      assertEquals(length, iterator.readValues(buffer));
      for (int j = 0; j < length; j++) {
        assertEquals(length, buffer[j]);
      }
    }
    } finally {
      FileUtils.deleteDirectory(dir);
    }
  }
  @Test
  public void test6CountAllValues() {    
    MultiFacetIterator iterator = compressedMultiArray.iterator();
    BigIntArray counts = new BigIntArray(10);
    for (int i = 0; i < 10000; i++) {
      iterator.count(counts, i);
    }
    assertEquals(1000, counts.get(0));
    assertEquals(1000, counts.get(1));
    assertEquals(5000, counts.get(5));
    assertEquals(9000, counts.get(9));
  }
  public int getBufferLength(int i) {
    int length = i % 10;
    if (length == 0) {
      length = 1;
    }
    return length;
  }
 
}
