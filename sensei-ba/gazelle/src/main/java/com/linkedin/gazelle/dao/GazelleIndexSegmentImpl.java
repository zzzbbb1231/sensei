package com.linkedin.gazelle.dao;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.DocIdSet;

import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.creators.MetadataCreator;
import com.linkedin.gazelle.utils.GazelleColumnMedata;
import com.linkedin.gazelle.utils.GazelleColumnType;
import com.linkedin.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.ForwardIndex;
import com.senseidb.ba.IndexSegment;

public class GazelleIndexSegmentImpl implements IndexSegment {
  private Map<String, ColumnType> _columnTypeMap;
  private HashMap<String, GazelleColumnMedata> _columnMetatdaMap;
  private HashMap<String, TermValueList> _termValueListMap;
  private HashMap<String, CompressedIntArray> _compressedIntArrayMap;
  private HashMap<String, GazelleForwardIndexImpl> _forwardIndexMap;
  private int _length;

  public GazelleIndexSegmentImpl(CompressedIntArray[] compressedIntArr, TermValueList[] termValueListArr, GazelleColumnMedata[] columnMetadataArr, int length) {
    MetadataCreator _metadataWriter = new MetadataCreator();
    HashMap<String, GazelleColumnMedata> tempMap = new HashMap<String, GazelleColumnMedata>(); 
    _columnMetatdaMap = new HashMap<String, GazelleColumnMedata>();
    _termValueListMap = new HashMap<String, TermValueList>();
    _compressedIntArrayMap = new HashMap<String, CompressedIntArray>();
    _columnTypeMap = new HashMap<String, ColumnType>();
    _forwardIndexMap = new HashMap<String, GazelleForwardIndexImpl>();
    _length = length;
    for (int i = 0; i < compressedIntArr.length; i++) {
      compressedIntArr[i].getStorage().rewind();
      _compressedIntArrayMap.put(columnMetadataArr[i].getName(), compressedIntArr[i]);
      _termValueListMap.put(columnMetadataArr[i].getName(), termValueListArr[i]);
      _columnTypeMap.put(columnMetadataArr[i].getName(), ColumnType.valueOf(columnMetadataArr[i].getColumnType().toString()));
      tempMap.put(columnMetadataArr[i].getName(), columnMetadataArr[i]);
    }

    for (String column : _compressedIntArrayMap.keySet()) {
      _columnMetatdaMap.put(column, _metadataWriter.getMetadataFor(column, _termValueListMap.get(column), tempMap.get(column).getColumnType(), length));
      _forwardIndexMap.put(column,new GazelleForwardIndexImpl(column, _compressedIntArrayMap.get(column), _termValueListMap.get(column), _columnMetatdaMap.get(column)));
    }
  }

  public GazelleIndexSegmentImpl(HashMap<String, GazelleColumnMedata> metadataMap, HashMap<String, CompressedIntArray> compressedIntArrayMap, HashMap<String, TermValueList> termValueListMap, int length) {
    _columnTypeMap = new HashMap<String, ColumnType>();
    _forwardIndexMap = new HashMap<String, GazelleForwardIndexImpl>();
    for (String column : metadataMap.keySet()) {
      _columnTypeMap.put(column, ColumnType.valueOf(metadataMap.get(column).getColumnType().toString()));
      _forwardIndexMap.put(column, new GazelleForwardIndexImpl(column, compressedIntArrayMap.get(column), termValueListMap.get(column), metadataMap.get(column)));
    }
    _columnMetatdaMap = metadataMap;
    _compressedIntArrayMap = compressedIntArrayMap;
    _termValueListMap = termValueListMap;
    _length = length;
  }

  public HashMap<String, GazelleColumnMedata> getColumnMetatdaMap() {
    return _columnMetatdaMap;
  }

  public void setColumnMetatdaMap(HashMap<String, GazelleColumnMedata> columnMetatdaMap) {
    this._columnMetatdaMap = columnMetatdaMap;
  }

  public HashMap<String, TermValueList> getTermValueListMap() {
    return _termValueListMap;
  }

  public void setTermValueListMap(HashMap<String, TermValueList> termValueListMap) {
    this._termValueListMap = termValueListMap;
  }

  public HashMap<String, CompressedIntArray> getCompressedIntArrayMap() {
    return _compressedIntArrayMap;
  }

  public void setCompressedIntArrayMap(HashMap<String, CompressedIntArray> compressedIntArrayMap) {
    this._compressedIntArrayMap = compressedIntArrayMap;
  }

  @Override
  public Map<String, ColumnType> getColumnTypes() {
    return _columnTypeMap;
  }

  @Override
  public TermValueList<?> getDictionary(String column) {
    return _termValueListMap.get(column);
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
    return _length;
  }

}
