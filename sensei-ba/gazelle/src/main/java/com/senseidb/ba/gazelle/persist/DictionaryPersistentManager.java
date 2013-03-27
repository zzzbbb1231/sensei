package com.senseidb.ba.gazelle.persist;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.creators.DictionaryCreator;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.StreamUtils;

public class DictionaryPersistentManager {
  public static Logger logger =
      Logger.getLogger(DictionaryPersistentManager.class);

  public static void flush(Map<String, ColumnMetadata> metadataMap, Map<String, TermValueList> termValueListMap, String basePath, FileSystemMode mode) throws IOException {
    flush(metadataMap, termValueListMap, basePath, mode, null);
  }

  public static void flush(Map<String, ColumnMetadata> metadataMap, Map<String, TermValueList> termValueListMap, String baseDirPath, FileSystemMode mode, FileSystem fs) throws IOException {
    for (String column : metadataMap.keySet()) {
      String dictFileName =
          baseDirPath + "/" + metadataMap.get(column).getName() + ".dict";
      ColumnType columnType = metadataMap.get(column).getColumnType();
      if (columnType.isMulti()) {
        columnType = columnType.getElementType();
      }
      TermValueList termValueList = termValueListMap.get(column);
      persistDictionary(mode, fs, dictFileName, columnType, termValueList);
    }
  }

  public static void persistDictionary(FileSystemMode mode, FileSystem fs, String dictFileName, ColumnType columnType,
      TermValueList termValueList) throws IOException, UnsupportedEncodingException {
    DataOutputStream ds = StreamUtils.getOutputStream(dictFileName, mode, fs);
    try {
      
      
      switch (columnType) {
        case STRING:
          TermStringList stringList =
              (TermStringList) termValueList;
          for (int i = 0; i < stringList.size(); i++) {
            String entry = stringList.get(i);
            byte[] entryInBytes = entry.getBytes("UTF8");
            ds.writeShort(entryInBytes.length);
            ds.write(entryInBytes);
          }
          break;
        case INT:
          TermIntList intList = (TermIntList) termValueList;
          for (int i = 0; i < intList.size(); i++) {
            ds.writeInt(intList.getPrimitiveValue(i));
          }
          break;
        case LONG:
          TermLongList longList = (TermLongList) termValueList;
          for (int i = 0; i < longList.size(); i++) {
            ds.writeLong(longList.getPrimitiveValue(i));
          }
          break;
        case FLOAT:
          TermFloatList floatList =
              (TermFloatList) termValueList;
          for (int i = 0; i < floatList.size(); i++) {
            ds.writeFloat(floatList.getPrimitiveValue(i));
          }
          break;
        default:
          throw new UnsupportedOperationException();
      }
    } finally {
      ds.close();
    }
  }

  public static TermValueList read(File dictionaryFile, ColumnType type, int dictionarySize) throws IOException {
    TermValueList list = null;
    FileInputStream fIs = null;
    DataInputStream dIs = null;
    try {
      fIs = new FileInputStream(dictionaryFile);
      dIs = new DataInputStream(new BufferedInputStream(fIs));
      if (type.isMulti()) {
          type = type.getElementType();
      }
      logger.debug("initializing the dictionary from file " + dictionaryFile.getCanonicalPath());
      switch (type) {
        case STRING:
          final String[] arr =  new String[dictionarySize];
          byte[] buffer = new byte[10000];
          TermStringList termStringList = new TermStringList(dictionarySize);
          for (int i = 0; i < dictionarySize; i++) {
            int length = dIs.readShort();
            if (length > buffer.length ) {
              buffer = new byte[2 * length];
            }
           
            int read = dIs.read(buffer, 0, length);
            String str = new String(buffer, 0, length, "UTF8");
            if (i != 0) {
              arr[i] = str;
              termStringList.add(str);
            } else {
              //arr[i] = "";
              termStringList.add(null);
            }           
          }
          list = termStringList;
          break;
        case LONG:
          final long[] longArr = new long[dictionarySize];
          for (int i = 0; i < dictionarySize; i++) {
            longArr[i] = dIs.readLong();
          }
          TermLongList termLongList =
              new TermLongList(dictionarySize, DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(type)) {
                public int size() {
                  return longArr.length;
                }
                @Override
                public Long getRawValue(int index) {
                  return super.getPrimitiveValue(index);
                }
              };
          Field longField = TermLongList.class.getDeclaredField("_elements");
          longField.setAccessible(true);
          longField.set(termLongList, longArr);
          list = termLongList;
          break;
        case FLOAT:
          final float[] floatArr = new float[dictionarySize];
          for (int i = 0; i < dictionarySize; i++) {
            floatArr[i] = dIs.readFloat();
          }
          TermFloatList termFloatList =
              new TermFloatList(dictionarySize, DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(type)) {
                @Override
                public Float getRawValue(int index) {
                  return   super.getPrimitiveValue(index);
                }
            
                public int size() {
                  return floatArr.length;
                }
              };
          Field floatField = TermFloatList.class.getDeclaredField("_elements");
          floatField.setAccessible(true);
          floatField.set(termFloatList, floatArr);
          list = termFloatList;

          break;
        case INT:
          final int[] intArr = new int[dictionarySize];
          for (int i = 0; i < dictionarySize; i++) {
            intArr[i] = dIs.readInt();
          }
          TermIntList termIntList =
              new TermIntList(dictionarySize, DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(type)) {
                @Override
                public Integer getRawValue(int index) {
                  // TODO Auto-generated method stub
                  return super.getPrimitiveValue(index);
                }
                public int size() {
                  return intArr.length;
                }
              };
          Field intField = TermIntList.class.getDeclaredField("_elements");
          intField.setAccessible(true);
          intField.set(termIntList, intArr);
          list = termIntList;
          break;
        default:
          throw new UnsupportedOperationException();
            
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (dIs != null) { 
        dIs.close();     
      }
    }
    return list;
  }
}
