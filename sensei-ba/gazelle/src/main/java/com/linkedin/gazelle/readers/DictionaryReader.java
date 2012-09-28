package com.linkedin.gazelle.readers;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.utils.ColumnType;

public class DictionaryReader {

  public static Logger logger = Logger.getLogger(DictionaryReader.class);
  
  @SuppressWarnings("rawtypes")

  public static TermValueList read(File dictionaryFile, ColumnType type, int dictionarySize) {
    TermValueList list = null;
    try {
      FileInputStream fIs = new FileInputStream(dictionaryFile);
      DataInputStream dIs = new DataInputStream(fIs);
      switch (type) {
        case STRING:
          TermStringList termStringList = new TermStringList();
          
          for (int i = 0; i < dictionarySize; i++) {
            int length = dIs.readShort();
            byte[] bytes = new byte[length];
            dIs.read(bytes);
            String str = new String(bytes, "UTF8");
            if (i != 0) {
              termStringList.add(str);
            } else {
              termStringList.add(null);
            }
            list = termStringList;
            list.seal();
          }
          break;
        case LONG:
          TermLongList termLongList = new TermLongList(Primitive2StringInterpreter.LONG);
          long[] longArr = new long[dictionarySize];
          for (int i = 0; i < dictionarySize; i++) {
            longArr[i] = dIs.readLong();
          }
          Field longField = TermLongList.class.getDeclaredField("_elements");
          longField.setAccessible(true);
          longField.set(termLongList, longArr);
          list = termLongList;
          break;
        case FLOAT:
          TermFloatList termFloatList = new TermFloatList(Primitive2StringInterpreter.FLOAT);
          float[] floatArr = new float[dictionarySize];
          for (int i = 0; i < dictionarySize; i++) {
            floatArr[i] = dIs.readFloat();
          }
          Field floatField = TermFloatList.class.getDeclaredField("_elements");
          floatField.setAccessible(true);
          floatField.set(termFloatList, floatArr);
          list = termFloatList;
          break;
        case INT:
          TermIntList termIntList = new TermIntList(Primitive2StringInterpreter.INT);
          int[] intArr = new int[dictionarySize];
          for (int i = 0; i < dictionarySize; i++) {
            intArr[i] = dIs.readInt();
          }
          Field intField = TermIntList.class.getDeclaredField("_elements");
          intField.setAccessible(true);
          intField.set(termIntList, intArr);
          list = termIntList;
          break;
        default:
          break;
      }
      
    } catch (IOException e) {
      logger.error(e);
    } catch (UnsupportedOperationException e) {
      logger.error(e);
    } catch (SecurityException e) {
      logger.error(e);
    } catch (NoSuchFieldException e) {
      logger.error(e);
    } catch (IllegalArgumentException e) {
      logger.error(e);
    } catch (IllegalAccessException e) {
      logger.error(e);
    }
    
    return list;
  }
  
  private static class Primitive2StringInterpreter {
    public static String INT = "0000000000";
    public static String LONG = "00000000000000000000";
    public static String FLOAT = "0000000000.00000";
  }
}
