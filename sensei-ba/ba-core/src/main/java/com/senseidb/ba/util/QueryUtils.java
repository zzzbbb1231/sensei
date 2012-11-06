package com.senseidb.ba.util;

public class QueryUtils {

  public static boolean isRangeQuery(String query) {
    return ( (query.contains("(") && query.contains(")")) || (query.contains("[") && query.contains("]")) );
  }
  
  public static boolean isInclusiveRangeQuery(String query) {
    return (query.contains("[") && query.contains("]"));
  }
  
  public static boolean isExclusiveRangeQuery(String query) {
    return (query.contains("(") && query.contains(")"));
  }
}
