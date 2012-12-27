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
import org.apache.log4j.Logger;

import com.senseidb.ba.util.IndexConverter;

public class CSVDataSource implements GazelleDataSource {
  private static Logger logger = Logger.getLogger(CSVDataSource.class);
  private final File csvFile;
  private String delimiter = ",";
  private Set<CSVIterator> iterators = new HashSet<CSVIterator>();
  
  public CSVDataSource(File csvFile) {
      this.csvFile = csvFile;
      
  }
  public CSVDataSource(File csvFile, String delimiter) {
    this.csvFile = csvFile;
    this.delimiter = delimiter;
    
}
  @Override
  public Iterator<Map<String, Object>> newIterator() {
      CSVIterator ret = new CSVIterator(csvFile, delimiter);
      iterators.add(ret);
      return ret;
  }

  @Override
  public void closeCurrentIterators() {
      for (CSVIterator iterator : iterators) {
          iterator.close();
      }
      
  }
  public static class CSVIterator extends AbstractFileIterator {
      private List<String> fields = new ArrayList<String>();
      private final String delimiter;
    
      public CSVIterator(File csvFile, String delimiter)  {
          super(csvFile);
          this.delimiter = delimiter;
         String firstLine;
         while (true) {
           firstLine = lineIterator.nextLine();
           if (StringUtils.isNotBlank(firstLine)) {
             break;
            
           }
         }
         for (String field: firstLine.split(delimiter)) {
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
               for (String value: line.split(delimiter)) {
                    
                    String field = fields.get(i);
                   /* if ("dim_memberIndustry".equals(field) && ! (transformValue(value) instanceof Number)) {
                      value = "0";
                    }*/
                    ret.put(field, transformValue(value));
                 
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
        try {
            if (value == null) {
                return null;
            }
        
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
        } catch (Exception ex) {
          logger.warn(ex.getMessage());
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
        if (!(Character.isDigit(str.charAt(i)) == true || str.charAt(i) == '.' || str.charAt(i) == ',')) {
            return false;
        }
    }
    return true;
}
}

