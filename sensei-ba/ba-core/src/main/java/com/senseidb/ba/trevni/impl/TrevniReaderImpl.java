package com.senseidb.ba.trevni.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.DocIdSet;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnValues;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.ForwardIndex;
import com.senseidb.ba.IndexSegment;
import com.senseidb.ba.trevni.DataType;


/*
 * This is in a work in progress. 
 * 
 * */
public class TrevniReaderImpl implements IndexSegment {
  private Map<String, Class<?>> _columnTypes;
  private HashMap<String, TermValueList> _dictionaryMap;
  private ColumnFileReader _columnReader;
  private ColumnFileMetaData _metadata;
  private HashMap<String, TrevniForwardIndex> _forwardIndexMap;
  private long _rowCount;

  /*
   * Takes in a file and constructs the index
   */
  public TrevniReaderImpl(File dir) throws IOException, ClassNotFoundException {
    File[] files = dir.listFiles();
    for (File f : files) {
      if (f.getName().endsWith(".trv")) {
        _columnReader = new ColumnFileReader(f);
        _rowCount = _columnReader.getRowCount();
      }
    }
    _columnTypes = new HashMap<String, Class<?>>();
    _metadata = _columnReader.getMetaData();
    String dimTypes = _metadata.getString("columnTypes");
    for (String dimType : dimTypes.split(",")) {
      String dimName = dimType.split(":")[0];
      String dimDataType = dimType.split(":")[1];
      _columnTypes.put(dimName, DataType.getClassFromStringType(dimDataType));
    }
    constructDictionary(dir.getAbsolutePath());
    constructForwardIndex();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void constructDictionary(String basePath) throws IOException {
    _dictionaryMap = new HashMap<String, com.browseengine.bobo.facets.data.TermValueList>();
    String[] dictArray = _metadata.getString("dictMapping").split(",");
    for (String dict : dictArray) {
      String dimName = dict.split(":")[0];
      String dictFileName = dict.split(":")[1];
      File file = new File(basePath + "/" + dictFileName);
      _dictionaryMap.put(dimName,  TrevniDictionary.createTermValueList(file, _columnTypes.get(dimName)));
    }
  }

  public void constructForwardIndex() throws IOException {
    _forwardIndexMap = new HashMap<String, TrevniForwardIndex>();
    for (String column : _columnTypes.keySet()) {
        ColumnValues<Integer> intReader = _columnReader.getValues(column);
        TrevniForwardIndex fIndex = new TrevniForwardIndex(intReader, _rowCount, _dictionaryMap.get(column));
        _forwardIndexMap.put(column, fIndex);
      
    }
  }

  @Override
  public Map<String, ColumnType> getColumnTypes() {
    return _columnTypes;
  }

  @Override
  public TermValueList getDictionary(String column) {
    return _dictionaryMap.get(column);
  }

  @Override
  public DocIdSet[] getInvertedIndex(String column) {
    return null;
  }

  @Override
  public ForwardIndex getForwardIndex(String column) {
    return _forwardIndexMap.get(column);
  }

  @Override
  public int getLength() {
    return (int)_rowCount;
  }

}
