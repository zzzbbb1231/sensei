package com.senseidb.ba.gazelle.persist;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ColumnMetadata;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.gazelle.creators.DictionaryCreator;

public class DictionaryPersistentManager {
  public static Logger logger =
      Logger.getLogger(DictionaryPersistentManager.class);

  public static void flushOnHadoop(Map<String, ColumnMetadata> metadataMap, Map<String, TermValueList> termValueListMap, String basePath, FileSystem fs) throws IOException {
    for (String column : metadataMap.keySet()) {
      String dictFileName = metadataMap.get(column).getName() + ".dict";
      Path outFile = new Path(basePath + "/" + dictFileName);
      FSDataOutputStream ds = fs.create(outFile);
      try {
        switch (metadataMap.get(column).getColumnType()) {
          case STRING:
            TermStringList stringList =
                (TermStringList) termValueListMap.get(column);
            for (int i = 0; i < stringList.size(); i++) {
              String entry = stringList.get(i);
              byte[] entryInBytes = entry.getBytes("UTF8");
              ds.writeShort(entryInBytes.length);
              ds.write(entryInBytes);
            }
            break;
          case INT:
            TermIntList intList = (TermIntList) termValueListMap.get(column);
            for (int i = 0; i < intList.size(); i++) {
              ds.writeInt(intList.getPrimitiveValue(i));
            }
            break;
          case LONG:
            TermLongList longList = (TermLongList) termValueListMap.get(column);
            for (int i = 0; i < longList.size(); i++) {
              ds.writeLong(longList.getPrimitiveValue(i));
            }
            break;
          case FLOAT:
            TermFloatList floatList =
                (TermFloatList) termValueListMap.get(column);
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
  }

  public static void flush(Map<String, ColumnMetadata> metadataMap, Map<String, TermValueList> termValueListMap, File baseDir) throws IOException {
    for (String column : metadataMap.keySet()) {
      String dictFileName = metadataMap.get(column).getName() + ".dict";
      if (!baseDir.exists()) {
        baseDir.mkdirs();
      }
      OutputStream out = new FileOutputStream(new File(baseDir, dictFileName));
      DataOutputStream ds = new DataOutputStream(out);
      try {
        switch (metadataMap.get(column).getColumnType()) {
          case STRING:
            TermStringList stringList =
                (TermStringList) termValueListMap.get(column);
            for (int i = 0; i < stringList.size(); i++) {
              String entry = stringList.get(i);
              byte[] entryInBytes = entry.getBytes("UTF8");
              ds.writeShort(entryInBytes.length);
              ds.write(entryInBytes);
            }
            break;
          case INT:
            TermIntList intList = (TermIntList) termValueListMap.get(column);
            for (int i = 0; i < intList.size(); i++) {
              ds.writeInt(intList.getPrimitiveValue(i));
            }
            break;
          case LONG:
            TermLongList longList = (TermLongList) termValueListMap.get(column);
            for (int i = 0; i < longList.size(); i++) {
              ds.writeLong(longList.getPrimitiveValue(i));
            }
            break;
          case FLOAT:
            TermFloatList floatList =
                (TermFloatList) termValueListMap.get(column);
            for (int i = 0; i < floatList.size(); i++) {
              ds.writeFloat(floatList.getPrimitiveValue(i));
            }
            break;
          default:
            throw new UnsupportedOperationException();
        }
      } finally {
        ds.close();
        out.close();
      }
    }
  }

  public static TermValueList read(File dictionaryFile, ColumnType type, int dictionarySize) throws IOException {
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
          final long[] longArr = new long[dictionarySize];
          for (int i = 0; i < dictionarySize; i++) {
            longArr[i] = dIs.readLong();
          }
          TermLongList termLongList =
              new TermLongList(DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(type)) {
                public int size() {
                  return longArr.length;
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
              new TermFloatList(DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(type)) {
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
              new TermIntList(DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(type)) {
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
          break;
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      dIs.close();
      fIs.close();
    }
    return list;
  }
}
