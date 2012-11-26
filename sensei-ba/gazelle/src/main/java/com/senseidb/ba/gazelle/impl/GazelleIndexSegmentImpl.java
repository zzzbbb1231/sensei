package com.senseidb.ba.gazelle.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.DocIdSet;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.IndexSegment;

public class GazelleIndexSegmentImpl implements IndexSegment {
  private Map<String, ColumnMetadata> columnMetatdaMap = new HashMap<String, ColumnMetadata>();
  private Map<String, TermValueList> termValueListMap = new HashMap<String, TermValueList>();
  private Map<String, ForwardIndex> forwardIndexMap = new HashMap<String, ForwardIndex>();
  private int length;
  private Map<String, ColumnType> columnTypes = new HashMap<String, ColumnType>();

  public GazelleIndexSegmentImpl(ForwardIndex[] forwardIndexArr, TermValueList[] termValueListArr, ColumnMetadata[] columnMetadataArr, int length) {
    this.length = length;
    for (int i = 0; i < forwardIndexArr.length; i++) {
      forwardIndexMap.put(columnMetadataArr[i].getName(), forwardIndexArr[i]);
      termValueListMap.put(columnMetadataArr[i].getName(), termValueListArr[i]);
      columnMetatdaMap.put(columnMetadataArr[i].getName(), columnMetadataArr[i]);
    }
    init();
  }
  public GazelleIndexSegmentImpl(Map<String, ColumnMetadata> metadataMap, Map<String, ForwardIndex> forwardIndexMap, Map<String, TermValueList> termValueListMap, int length) {
    this.forwardIndexMap = forwardIndexMap;
    this.columnMetatdaMap = metadataMap;
    this.termValueListMap = termValueListMap;
    this.length = length;
    init();
  }
  public GazelleIndexSegmentImpl() {
  }
  private void init() {
    columnTypes = new HashMap<String, ColumnType>();
    for (String columnName : columnMetatdaMap.keySet()) {
      columnTypes.put(columnName, columnMetatdaMap.get(columnName).getColumnType());
      
    }
    
  }


  public Map<String, ColumnMetadata> getColumnMetadataMap() {
    return columnMetatdaMap;
  }
  public Map<String, TermValueList> getDictionaries() {
    return termValueListMap;
  }
  public Map<String, ForwardIndex> getForwardIndexes() {
    return forwardIndexMap;
  }
  @Override
  public Map<String, ColumnType> getColumnTypes() {
    return columnTypes;
  }

  @Override
  public TermValueList<?> getDictionary(String column) {
    return termValueListMap.get(column);
  }

  @Override
  public DocIdSet[] getInvertedIndex(String column) {
    return null;
  }

  @Override
  public ForwardIndex getForwardIndex(String column) {
    return forwardIndexMap.get(column);
  }

  @Override
  public int getLength() {
    return length;
  }
  public void setLength(int length) {
    this.length = length;
  }
  
}
