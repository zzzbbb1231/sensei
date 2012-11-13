package com.senseidb.ba.format;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

public class CSVDataSource implements GazelleDataSource {
  private final File csvFile;
  private Set<CSVIterator> iterators = new HashSet<CSVIterator>();
  public CSVDataSource(File csvFile) {
      this.csvFile = csvFile;
      
  }
  @Override
  public Iterator<Map<String, Object>> newIterator() {
      CSVIterator ret = new CSVIterator(csvFile);
      iterators.add(ret);
      return ret;
  }

  @Override
  public void closeCurrentIterators() {
      for (CSVIterator iterator : iterators) {
          iterator.close();
      }
      
  }
  public static class CSVIterator extends AbstractIterator {
      private List<String> fields = new ArrayList<String>();
    
      public CSVIterator(File csvFile)  {
          super(csvFile);
         String firstLine;
         while (true) {
           firstLine = lineIterator.nextLine();
           if (StringUtils.isNotBlank(firstLine)) {
             break;
            
           }
         }
         for (String field: firstLine.split(",")) {
           if (StringUtils.isNotBlank(field)) {
             fields.add(field.trim());
           }
         }
      }     
      protected Map<String, Object> processLine(String line) {
          if (line != null && !StringUtils.isEmpty(line)) {
            Map<String, Object> ret = new HashMap<String, Object>(fields.size());
             try {
                int i = 0; 
               for (String value: line.split(",")) {
                 
                    ret.put(fields.get(i), transformValue(value));
                 
                  i++;
                }
                  return ret;
              } catch (Exception e) {
                  throw new RuntimeException(e);
              }
          }
          return null;
      }
      private Object transformValue(String value) {        
        if (value.length() > 0 && (StringUtils.isNumeric(value)  || ( value.startsWith("-") && value.length() > 1 && StringUtils.isNumeric(value.substring(1))))) {
          long lng = Long.parseLong(value);
          if (lng <= Integer.MAX_VALUE && lng >= Integer.MIN_VALUE) {
            return Integer.valueOf((int)lng);
          } else {
            return lng;
          }
        }
        if (isFloat(value)) {
          double parseDouble = Double.parseDouble(value);
          return Float.valueOf((float)parseDouble);
        }
        
        return value;
      }
  }
  public static boolean isFloat(String str) {
    if (str == null) {
        return false;
    }
    int sz = str.length();
    if (sz == 0) {
      return false;
    }
    if (str.charAt(0) != '-' && !Character.isDigit(str.charAt(0))) {
      return false;
    }
    for (int i = 1; i < sz; i++) {
        if (Character.isDigit(str.charAt(i)) == false && (str.charAt(i) != '.' && str.charAt(i) == ',')) {
            return false;
        }
    }
    return true;
}
}

