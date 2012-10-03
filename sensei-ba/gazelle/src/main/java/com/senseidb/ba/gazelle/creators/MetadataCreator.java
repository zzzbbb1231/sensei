package com.senseidb.ba.gazelle.creators;

import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.gazelle.utils.GazelleColumnMetadata;
import com.senseidb.ba.gazelle.utils.GazelleColumnType;

/**
 * @author dpatel
 */

public class MetadataCreator {
  private static Logger logger = Logger.getLogger(MetadataCreator.class);
  private long _startOffset = 0;

  public GazelleColumnMetadata getMetadataFor(String column, TermValueList list, GazelleColumnType type, int numOfElements) {
    GazelleColumnMetadata metadata = new GazelleColumnMetadata();
    int numOfBits = CompressedIntArray.getNumOfBits(list.size());
    int bufferSize = CompressedIntArray.getRequiredBufferSize(numOfElements, numOfBits);
    metadata.setName(column);
    metadata.setColumnType(type);
    metadata.setBitsPerElement(numOfBits);
    metadata.setByteLength(bufferSize);
    metadata.setNumberOfDictionaryValues(list.size());
    metadata.setNumberOfElements(numOfElements);
    metadata.setStartOffset(_startOffset);
    metadata.setSorted(false);
    _startOffset += bufferSize;
    return metadata;
  }
  
}
