package com.linkedin.gazelle.flushers;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.utils.GazelleColumnMetadata;

public class DictionaryFlusher {

  public static void flush(HashMap<String, GazelleColumnMetadata> metadataMap, HashMap<String, TermValueList> termValueListMap, String baseDir) throws IOException {
    for (String column : metadataMap.keySet()) {
      String dictFileName = metadataMap.get(column).getName() + ".dict";
      OutputStream out = new FileOutputStream(new File(baseDir, dictFileName));
      DataOutputStream ds = new DataOutputStream(out);
      try {
        switch (metadataMap.get(column).getColumnType()) {
        case STRING:
          TermStringList stringList = (TermStringList) termValueListMap.get(column);
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
          TermFloatList floatList = (TermFloatList) termValueListMap.get(column);
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
}
