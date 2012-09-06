package com.senseidb.ba.trevni.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import com.senseidb.ba.trevni.*;

import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnValues;


/*
 * This is in a work in progress. 
 * 
 * */
public class TrevniReaderImpl implements PinotIndexReader {
  private Map<String, Class<?>> _columnTypes;
  private HashMap<String, TrevniDictionary> _dictionaryMap;
  private ColumnFileReader _columnReader;
  private ColumnFileMetaData _metadata;
  private String[] _dimensions;
  private String[] _dimTypes;
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
    String dimTypes = _metadata.getString("dimTypes");
    for (String dimType : dimTypes.split(",")) {
      String dimName = dimType.split(":")[0];
      String dimDataType = dimType.split(":")[1];
      _columnTypes.put(dimName, DataType.getClassFromStringType(dimDataType));
    }
    _dimensions = _metadata.getString("orderedDimNames").split(",");
    constructForwardIndex();
    constructDictionary(dir.getAbsolutePath());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public void constructDictionary(String basePath) throws IOException {
    _dictionaryMap = new HashMap<String, TrevniDictionary>();
    String[] dictArray = _metadata.getString("dictMapping").split(",");
    for (String dict : dictArray) {
      String dimName = dict.split(":")[0];
      String dictFileName = dict.split(":")[1];
      File file = new File(basePath + "/" + dictFileName);
      _dictionaryMap.put(dimName, new TrevniDictionary(file, _columnTypes.get(dimName)));
    }
  }

  public void constructForwardIndex() throws IOException {
    _forwardIndexMap = new HashMap<String, TrevniForwardIndex>();
    
    for (String dim : _dimensions) {
      if (dim.startsWith("shrd") || dim.startsWith("sort") || dim.startsWith("dim")) {
        ColumnValues<Integer> intReader = _columnReader.getValues(dim);
        TrevniForwardIndex fIndex = new TrevniForwardIndex(intReader, "dim", _rowCount);
        _forwardIndexMap.put(dim, fIndex);
      } else if (dim.startsWith("time")) {
        ColumnValues<Integer> longReader = _columnReader.getValues(dim);
        TrevniForwardIndex fIndex = new TrevniForwardIndex(longReader, "time", _rowCount);
        _forwardIndexMap.put(dim, fIndex);
      } else {
        ColumnValues<Integer> doubleReader = _columnReader.getValues(dim);
        TrevniForwardIndex fIndex = new TrevniForwardIndex(doubleReader, "met", _rowCount);
        _forwardIndexMap.put(dim, fIndex);
      }
    }
  }

  @Override
  public Map<String, Class<?>> getColumnTypes() {
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
