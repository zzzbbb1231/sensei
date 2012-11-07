package com.senseidb.ba.util;

public class QueryUtils {

  public static boolean isRangeQuery(String query) {
    return ( ((query.startsWith("(") && query.endsWith(")")) || (query.startsWith("[") && query.endsWith("]"))) && query.contains("TO"));
  }
  
  public static boolean isInclusiveRangeQuery(String query) {
    return ((query.startsWith("[") && query.endsWith("]")) && query.contains("TO"));
  }
  
  public static boolean isExclusiveRangeQuery(String query) {
    return (query.startsWith("(") && query.endsWith(")") && query.contains("TO"));
  }
}
