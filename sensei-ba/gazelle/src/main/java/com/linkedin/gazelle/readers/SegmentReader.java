package com.linkedin.gazelle.readers;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.utils.ColumnMedata;
import com.linkedin.gazelle.utils.CompressedIntArray;
import com.linkedin.gazelle.utils.ReadMode;

public class SegmentReader {
  private static String METADATA_FILE_NAME = "metadata.properties";
  private static String FORWARD_INDEX_FILE_NAME = "gazelle.fIdx";
  private static Logger logger = Logger.getLogger(SegmentReader.class);

  private HashMap<String, ColumnMedata> _metadataMap;
  private static ReadMode _mode;

  private File _indexDir;

  public SegmentReader(File dir) throws ConfigurationException {
    _indexDir = dir;
    File file = new File(_indexDir, METADATA_FILE_NAME);
    setUpColumnMetadataMetadata(new PropertiesConfiguration(file));
    _mode = ReadMode.MMAPPED;
  }

  public SegmentReader(File dir, ReadMode mode) throws ConfigurationException {
    _indexDir = dir;
    File file = new File(_indexDir, METADATA_FILE_NAME);
    setUpColumnMetadataMetadata(new PropertiesConfiguration(file));
    _mode = mode;
  }

  private void setUpColumnMetadataMetadata(PropertiesConfiguration config) throws ConfigurationException {
    _metadataMap = ColumnMedata.readFromFile(config);
  }

  public HashMap<String, ColumnMedata> readColumnMetadataMap() {
    return _metadataMap;
  }

  public HashMap<String, TermValueList> readDictionaryMap() {
    HashMap<String, TermValueList> dictionaryMap = new HashMap<String, TermValueList>();
    for (String column : _metadataMap.keySet()) {
      File file = new File(_indexDir, (column + ".dict"));
      TermValueList list =
          DictionaryReader.read(file, _metadataMap.get(column).getOriginalType(), _metadataMap.get(column)
              .getNumberOfDictionaryValues());
      dictionaryMap.put(column, list);
    }
    return dictionaryMap;
  }

  public HashMap<String, CompressedIntArray> readForwardIndexMap() throws IOException {
    HashMap<String, CompressedIntArray> forwardIndexMap = new HashMap<String, CompressedIntArray>();

    for (String column : _metadataMap.keySet()) {
      File file = new File(_indexDir, FORWARD_INDEX_FILE_NAME);
      forwardIndexMap.put(column, ForwardIndexReader.readForwardIndex(_metadataMap.get(column), file, _mode));
    }
    return forwardIndexMap;
  }

}
