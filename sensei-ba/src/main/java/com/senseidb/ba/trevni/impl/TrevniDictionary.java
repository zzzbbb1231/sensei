package com.senseidb.ba.trevni.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnValues;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.DictionaryCreator;

public class TrevniDictionary {

  public static TermValueList createTermValueList(File file, ColumnType originalValType) throws IOException {

    ColumnFileReader reader = new ColumnFileReader(file);
    int length = (int) reader.getColumnCount();
    DictionaryCreator dictionaryCreator = new DictionaryCreator();
    ColumnValues<String> val = reader.getValues(0);

    if (originalValType == ColumnType.INT) {
      while (val.hasNext()) {
        String[] pair = val.next().split(":");
        String original = pair[0];
        Integer mapped = Integer.parseInt(pair[1]);
        if (mapped != 0) {
          dictionaryCreator.addIntValue(Integer.parseInt(original));
        }
      }
      return dictionaryCreator.produceIntDictionary();
    } else if (originalValType == ColumnType.LONG) {
      while (val.hasNext()) {
        String[] pair = val.next().split(":");
        String original = pair[0];
        Integer mapped = Integer.parseInt(pair[1]);
        if (mapped != 0) {
          dictionaryCreator.addLongValue(Long.parseLong(original));

        }
      }
      return dictionaryCreator.produceLongDictionary();
    } else if (originalValType == ColumnType.STRING) {
      while (val.hasNext()) {
        String next = val.next();
        String[] pair = next.split(":");
        String original = pair[0];
        Integer mapped = Integer.parseInt(pair[1]);
        if (mapped != 0) {
          dictionaryCreator.addStringValue(original);
        }
      }
      return dictionaryCreator.produceStringDictionary();
    } else {
      throw new UnsupportedOperationException(originalValType.toString());
    }
  }

}
