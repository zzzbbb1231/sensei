package com.senseidb.ba.gazelle.creators;

import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ColumnMetadata;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.gazelle.utils.CompressedIntArray;

/**
 * @author dpatel
 */

public class MetadataCreator {

  public  static ColumnMetadata createMetadata(String column, TermValueList list, ColumnType type, int numOfElements, boolean sorted) {
    ColumnMetadata metadata = new ColumnMetadata();
    if (!sorted) {
    int numOfBits = CompressedIntArray.getNumOfBits(list.size());
    int bufferSize = (int)CompressedIntArray.getRequiredBufferSize(numOfElements, numOfBits);
      metadata.setBitsPerElement(numOfBits);
      metadata.setByteLength(bufferSize);
    } else {
      metadata.setBitsPerElement(-1);
      metadata.setByteLength(-1);

    }
    metadata.setSorted(sorted);
    metadata.setName(column);
    metadata.setColumnType(type);
    metadata.setNumberOfDictionaryValues(list.size());
    metadata.setNumberOfElements(numOfElements);
    return metadata;
  }
  public  static ColumnMetadata createMultiMetadata(String column, TermValueList list, ColumnType type, int numOfElements) {
      ColumnMetadata metadata = new ColumnMetadata();
      int numOfBits = CompressedIntArray.getNumOfBits(list.size());
        metadata.setBitsPerElement(numOfBits);
        metadata.setByteLength(-1);
        metadata.setMulti(true);
      metadata.setSorted(false);
      metadata.setName(column);
      metadata.setColumnType(type);
      metadata.setNumberOfDictionaryValues(list.size());
      metadata.setNumberOfElements(numOfElements);
      return metadata;
    }
}
