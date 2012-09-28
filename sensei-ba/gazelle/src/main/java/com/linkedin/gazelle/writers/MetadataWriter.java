package com.linkedin.gazelle.writers;

import java.io.File;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.utils.ColumnMedata;
import com.linkedin.gazelle.utils.CompressedIntArray;

/**
 * @author dpatel
 */

public class MetadataWriter {
  private static Logger logger = Logger.getLogger(MetadataWriter.class);
  private ColumnMedata[] _metadataArr;

  public MetadataWriter(ColumnMedata[] columnMetadataArr, TermValueList[] termValueListArr, CompressedIntArray[] compressedIntArrays, int count) {
    setUpMetadata(columnMetadataArr, termValueListArr, compressedIntArrays, count);
  }

  private void setUpMetadata(ColumnMedata[] columnMetadataArr, TermValueList[] termValueListArr, CompressedIntArray[] compressedIntArrays, int count) {
    long startOffset = 0;
    for (int i = 0; i < columnMetadataArr.length; i++) {
      int numOfBits = CompressedIntArray.getNumOfBits(termValueListArr[i].size());
      int bufferSize = CompressedIntArray.getRequiredBufferSize(count, numOfBits);
      columnMetadataArr[i].setBitsPerElement(numOfBits);
      columnMetadataArr[i].setByteLength(bufferSize);
      columnMetadataArr[i].setNumberOfDictionaryValues(termValueListArr[i].size());
      columnMetadataArr[i].setNumberOfElements(count);
      columnMetadataArr[i].setStartOffset(startOffset);
      columnMetadataArr[i].setSorted(false);
      startOffset += bufferSize;
    }
    _metadataArr = columnMetadataArr;
  }

  public ColumnMedata[] getMetadataArr() {
    return _metadataArr;
  }

  public void flush(String basePath) {
    try {
      PropertiesConfiguration config = new PropertiesConfiguration(new File(basePath, "metadata.properties"));
      for (int i =0; i < _metadataArr.length ; i++) {
        _metadataArr[i].addToConfig(config);
      }
      config.save();
    } catch (ConfigurationException e) {
      logger.error(e);
    }

  }
}
