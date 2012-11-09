package com.senseidb.ba.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.senseidb.ba.util.QueryUtils.RangeType;

public class QueryUtilsTest {
  
  @Test
  public void testIsRangeQuery() throws Exception {
    assertEquals(true, QueryUtils.isRangeQuery("[1 to 400]"));
    assertEquals(true, QueryUtils.isRangeQuery("(1 to 400)"));
    assertEquals(true, QueryUtils.isRangeQuery("(1 to 400]"));
    assertEquals(true, QueryUtils.isRangeQuery("[1 to 400)"));
    assertEquals(true, QueryUtils.isRangeQuery("[1 TO 400]"));
    
    assertEquals(false, QueryUtils.isRangeQuery("1 TO 400]"));
    assertEquals(false, QueryUtils.isRangeQuery("[1 TO 400"));
    assertEquals(false, QueryUtils.isRangeQuery("1 TO 400"));
  }
  
  @Test 
  public void testStartIndexRangeType() {
    assertEquals(RangeType.EXCLUSIVE, QueryUtils.getStarIndexRangeType("(1 to 400)"));
    assertEquals(RangeType.EXCLUSIVE, QueryUtils.getStarIndexRangeType("(1 to 400]"));
    
    assertEquals(RangeType.INCLUSIVE, QueryUtils.getStarIndexRangeType("[1 to 200]"));
    assertEquals(RangeType.INCLUSIVE, QueryUtils.getStarIndexRangeType("[1 to 200)"));
  }
  
  @Test 
  public void testEndIndexRangeType() {
    assertEquals(RangeType.EXCLUSIVE, QueryUtils.getEndIndexRangeType("(1 to 400)"));
    assertEquals(RangeType.EXCLUSIVE, QueryUtils.getEndIndexRangeType("[1 to 400)"));
    
    assertEquals(RangeType.INCLUSIVE, QueryUtils.getEndIndexRangeType("[1 to 400]"));
    assertEquals(RangeType.INCLUSIVE, QueryUtils.getEndIndexRangeType("(1 to 400]"));
  }
  
}
