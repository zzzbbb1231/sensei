package com.senseidb.ba.util;

import com.senseidb.ba.facet.ZeusDataCache;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;

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
  
  public static int[] getRangeIndexes(final ZeusDataCache zeusDataCache, final String value, final String[] values) {
    int [] rangeIndexArr = new int[2];
    
    
    if (values[0].equals("*")) {
      rangeIndexArr[0] = 0;
    } else {
      rangeIndexArr[0]= zeusDataCache.getDictionary().indexOf(values[0]);
    }
    if (values[1].equals("*")) {
      rangeIndexArr[1] = zeusDataCache.getDictionary().size() - 1;
    } else {
      rangeIndexArr[1] = zeusDataCache.getDictionary().indexOf(values[1]);
    }
    
    if (rangeIndexArr[0] < 0) {
      rangeIndexArr[0] = -(rangeIndexArr[0] + 1);
    } else {
      switch(QueryUtils.getStarIndexRangeType(value)) {
        case EXCLUSIVE:
          if (!values[0].equals("*")) {
            rangeIndexArr[0] += 1;
          }
        break;
      }
    }
    
    if (rangeIndexArr[1] < 0) {
      rangeIndexArr[1] = -(rangeIndexArr[1] + 1);
      rangeIndexArr[1] = Math.max(0, rangeIndexArr[1] - 1);
    } else {
      switch(QueryUtils.getEndIndexRangeType(value)) {
        case EXCLUSIVE:
          if (!values[1].equals("*")) {
            rangeIndexArr[1] -= 1;
          }
        break;  
      }
    }
    
    return rangeIndexArr;
  }
  public static int[] getRangeIndexes(final DictionarySnapshot dictionarySnapshot, final String value, final String[] values) {
    int [] rangeIndexArr = new int[2];
    
    
    if (values[0].equals("*")) {
      rangeIndexArr[0] = 0;
    } else {
      rangeIndexArr[0]= dictionarySnapshot.sortedIndexOf(values[0]);
    }
    if (values[1].equals("*")) {
      rangeIndexArr[1] = dictionarySnapshot.size() - 1;
    } else {
      rangeIndexArr[1] = dictionarySnapshot.sortedIndexOf(values[1]);
    }
    
    if (rangeIndexArr[0] < 0) {
      rangeIndexArr[0] = -(rangeIndexArr[0] + 1);
    } else {
      switch(QueryUtils.getStarIndexRangeType(value)) {
        case EXCLUSIVE:
          if (!values[0].equals("*")) {
            rangeIndexArr[0] += 1;
          }
        break;
      }
    }
    
    if (rangeIndexArr[1] < 0) {
      rangeIndexArr[1] = -(rangeIndexArr[1] + 1);
      rangeIndexArr[1] = Math.max(0, rangeIndexArr[1] - 1);
    } else {
      switch(QueryUtils.getEndIndexRangeType(value)) {
        case EXCLUSIVE:
          if (!values[1].equals("*")) {
            rangeIndexArr[1] -= 1;
          }
        break;  
      }
    }
    
    return rangeIndexArr;
  }
  public static enum RangeType {
    INCLUSIVE, EXCLUSIVE;
  }
}
