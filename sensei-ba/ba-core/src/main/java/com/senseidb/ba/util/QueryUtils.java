package com.senseidb.ba.util;

public class QueryUtils {
 
  public static boolean isRangeQuery(String query) {
    if (query == null || query.length() < 1) {
      return false;
    }
    
    return ((query.startsWith("[") || query.startsWith("(")) && (query.endsWith("]") || query.endsWith(")")) && (query.contains("TO") || query.contains("to")));
  }
  
  public static RangeType getStarIndexRangeType(String query) {
    if (query.startsWith("[")) {
      return RangeType.INCLUSIVE;
    } else {
      return RangeType.EXCLUSIVE;
    }
  }
  
  public static RangeType getEndIndexRangeType(String query) {
    if (query.endsWith("]")) {
      return RangeType.INCLUSIVE;
    } else {
      return RangeType.EXCLUSIVE;
    }
  }
  
  public static enum RangeType {
    INCLUSIVE, EXCLUSIVE;
  }
}
