package com.senseidb.ba.gazelle.readers;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.log4j.Logger;
import org.mortbay.io.RuntimeIOException;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.utils.GazelleColumnType;
import com.senseidb.ba.gazelle.utils.GazelleUtils;

public class DictionaryReader {

  public static Logger logger = Logger.getLogger(DictionaryReader.class);
  
  @SuppressWarnings("rawtypes")

  public static TermValueList read(File dictionaryFile, GazelleColumnType type, int dictionarySize) throws IOException {
    TermValueList list = null;
    FileInputStream fIs = null;
    DataInputStream dIs = null;
    try {
      fIs = new FileInputStream(dictionaryFile);
       dIs = new DataInputStream(fIs);
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
          TermLongList termLongList = new TermLongList(GazelleUtils.LONG_STRING_REPSENTATION);
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
          TermFloatList termFloatList = new TermFloatList(GazelleUtils.FLOAT_STRING_REPSENTATION);
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
          TermIntList termIntList = new TermIntList(GazelleUtils.INT_STRING_REPSENTATION);
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
      
    } catch (Exception e) {
      logger.error(e);
      throw new RuntimeIOException(e);
    } finally {
      dIs.close();
      fIs.close();
    }
    return list;
  }
}
