package com.senseidb.ba.gazelle.utils.multi;


import static org.junit.Assert.*;
import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.senseidb.ba.gazelle.utils.ReadMode;

public class CompressedMultiArrayChunkTest {

  private CompressedMultiArrayChunk compressedMultiArrayChunk;
  private int[] buffer;

  @Before
  public void setUp() throws Exception {
     compressedMultiArrayChunk = new CompressedMultiArrayChunk(0,10, 1000);
     for (int i = 0 ; i < 10000; i ++) {
       int length = i % 10;
       int[] arr = new int[length];
       Arrays.fill(arr, i % 10);
       compressedMultiArrayChunk.add(arr);      
     }  
     compressedMultiArrayChunk.initSkipLists(); 
     buffer = new int[10];
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test1AddElements() {    
    assertEquals(compressedMultiArrayChunk.getOffsets().length, compressedMultiArrayChunk.getSkipList().length);
    assertEquals(23, compressedMultiArrayChunk.getSkipList().length);
    assertEquals(0, compressedMultiArrayChunk.getOffsets()[0]);
    assertEquals(0, compressedMultiArrayChunk.getSkipList()[0]);
    assertEquals(10000, compressedMultiArrayChunk.getOpenBitSet().cardinality());
    assertEquals(46000, compressedMultiArrayChunk.getCurrentSize());
  }
 
  @Test
  public void test3ReadAllValues() {    
    MultiFacetIterator iterator = compressedMultiArrayChunk.iterator();
    
    for (int i = 0; i < 10000; i++) {
     
      assertTrue(iterator.advance(i));
      int length = i % 10;
      assertEquals(length, iterator.readValues(buffer));
      for (int j = 0; j < length; j++) {
        assertEquals(length, buffer[j]);
      }
    }
  }
  @Test
  public void test3bRandomReadAllValues() {    
    
    for (int i = 0; i < 10000; i++) {
     
      int length = i % 10;
      assertEquals(length, compressedMultiArrayChunk.randomRead(buffer, i));
      for (int j = 0; j < length; j++) {
        assertEquals(length, buffer[j]);
      }
    }
    assertEquals(0, compressedMultiArrayChunk.randomRead(buffer, 10000));
  }
  @Test
  public void test4ReadAllValuesWith1000Increment() {    
    MultiFacetIterator iterator = compressedMultiArrayChunk.iterator();    
    for (int i = 0; i < 10000; i+=1000) {     
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
    MultiFacetIterator iterator = compressedMultiArrayChunk.iterator();
    assertFalse(iterator.advance(10001));  
  }
  @Test
  public void test5ReadValuesFromTheNextElementInTheSkipList() {    
    MultiFacetIterator iterator = compressedMultiArrayChunk.iterator();
    assertTrue(iterator.advance(2679));
    iterator = compressedMultiArrayChunk.iterator();
    assertTrue(iterator.advance(449));
    int length = 449 % 10;
    assertEquals(length, iterator.readValues(buffer));
    for (int j = 0; j < length; j++) {
      assertEquals(length, buffer[j]);
    }
  }
  @Test
  public void test6ReadAllValuesWith1000IncrementAfterPersistingAndReadingThefile() throws Exception {    
    File dir = new File("temp");
    try {
    FileUtils.deleteDirectory(dir);
    dir.mkdirs();
    File file = new File(dir, "temp");
    file.createNewFile();
    int[] offsets = compressedMultiArrayChunk.getOffsets();
    int[] skipList = compressedMultiArrayChunk.getSkipList();
    compressedMultiArrayChunk.flush(file);
    compressedMultiArrayChunk = CompressedMultiArrayChunk.readFromFile(10, file, ReadMode.DBBuffer);
    assertTrue(Arrays.equals(offsets, compressedMultiArrayChunk.getOffsets()));
    assertTrue(Arrays.equals(skipList, compressedMultiArrayChunk.getSkipList()));
    MultiFacetIterator iterator = compressedMultiArrayChunk.iterator();
    
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
  public int getBufferLength(int i) {
    int length = i % 10;
    if (length == 0) {
      length = 1;
    }
    return length;
  }
 
}
