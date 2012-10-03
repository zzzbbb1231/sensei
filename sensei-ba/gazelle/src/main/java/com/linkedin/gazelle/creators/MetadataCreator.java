package com.linkedin.gazelle.creators;

import java.io.File;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.utils.GazelleColumnMetadata;
import com.linkedin.gazelle.utils.GazelleColumnType;
import com.linkedin.gazelle.utils.CompressedIntArray;

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
