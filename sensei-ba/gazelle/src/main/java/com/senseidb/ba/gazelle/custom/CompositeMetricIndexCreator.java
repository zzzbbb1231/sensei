package com.senseidb.ba.gazelle.custom;

import java.io.DataOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.creators.DictionaryCreator;
import com.senseidb.ba.gazelle.persist.DictionaryPersistentManager;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.HeapCompressedIntArray;
import com.senseidb.ba.gazelle.utils.OffHeapCompressedIntArray;
import com.senseidb.ba.gazelle.utils.StreamUtils;

public class CompositeMetricIndexCreator implements GazelleCustomIndexCreator {
  private final ColumnType columnType;
  private final List<String> columns;
  private Map<String, Integer> columnIndexes = new HashMap<String, Integer>();

  private DictionaryCreator dictionaryCreator;
  private String baseDir;
  private FileSystemMode mode;
  private FileSystem fs;
  private CompositeMetricsIndexStreamer indexStreamer;
  
  public CompositeMetricIndexCreator(List<String> columns, ColumnType columnType) {
    this.columns = columns;
    this.columnType = columnType;

    dictionaryCreator = new DictionaryCreator();
    Collections.sort(columns);
    int i = 0;
    for (String column : columns) {
      columnIndexes.put(column, i);
      i++;
    }
  }
  
  
  public void init(String baseDir, FileSystemMode mode, FileSystem fs) {
    this.baseDir = baseDir;
    this.mode = mode;
    this.fs = fs;
    
  }
  int valueCounter;
  private TermValueList<?> dictionary;

  @Override
  public void addValueToDictionary(String column, Object value) {
    valueCounter++;
    dictionaryCreator.addValue(value, columnType);
  }

  private int currentDocumentIndex;
  private PropertiesConfiguration properties;

  @Override
  public void addToForwardIndex(String column, Object value) {   
    int valId = dictionaryCreator.getIndex(value, columnType);
    indexStreamer.addValue(currentDocumentIndex,  columnIndexes.get(column), valId);
  }

  @Override
  public void newDocumentOnForwardIndex(int docId) {
    currentDocumentIndex = docId;
  }
  boolean flushed = false;
  @Override
  public void flushIndex(String baseDir, FileSystemMode mode, FileSystem fs) {
    if (!flushed) {
      flushed = true;
    } else {
      return;
    }
    indexStreamer.flush();
    String filePath = baseDir + "/compositeMetricIndexes.dict";
    DataOutputStream ds = null;
    try {
      if (mode == FileSystemMode.HDFS) {
        if (fs.exists(new Path(filePath))) {
          return;
        }
      }
      ds = StreamUtils.getOutputStream(filePath, mode, fs);
      DictionaryPersistentManager.persistDictionary(mode, fs, filePath, columnType, dictionary);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }   
  }

  public int getLength() {
    return valueCounter / columns.size();
  }

  public TermValueList<?> getDictionary() {
    return dictionary;
  }

  public ColumnType getColumnType() {
    return columnType;
  }
  private boolean propertiesSaved = false;
  @Override
  public void saveToProperties(PropertiesConfiguration configToBeAppended) {
    if (!propertiesSaved) {
      propertiesSaved = true;
    } else {
      return;
    }
   
    for (String column : columns) {
      ColumnMetadata columnMetadata = new ColumnMetadata(column, columnType);
      int numOfBits = OffHeapCompressedIntArray.getNumOfBits(dictionary.size());
      columnMetadata.setBitsPerElement(numOfBits);
      columnMetadata.setCustomIndexerType(CompositeMetricCustomIndex.class.getCanonicalName());
      columnMetadata.setNumberOfDictionaryValues(dictionary.size());
      columnMetadata.setByteLength(HeapCompressedIntArray.size(valueCounter, numOfBits) * 8);
      columnMetadata.setNumberOfElements(valueCounter /  columns.size());
      columnMetadata.addToConfig(configToBeAppended);
    }
  }

  @Override
  public void buildDictionary() {
    if (dictionary != null) {
      return;
    }    
    currentDocumentIndex = 0;
    dictionary = dictionaryCreator.produceDictionary();
    indexStreamer = new CompositeMetricsIndexStreamer(baseDir, mode, fs, valueCounter /  columns.size(), columns.size(), dictionary.size());
  }

  @Override
  public void createForwardIndex() {
  }

}
