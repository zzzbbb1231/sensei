package com.senseidb.ba.realtime;

import java.util.Map;

import org.apache.lucene.search.DocIdSet;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.IndexSegment;

public class RealtimeSnapshotIndexSegment implements IndexSegment {
  private int length;
  private Map<String, ColumnSearchSnapshot> columnSnapshots;
  private Map<String, ColumnType> columnTypes;
  
  
  
  public RealtimeSnapshotIndexSegment(int length, Map<String, ColumnSearchSnapshot> columnSnapshots, Map<String, ColumnType> columnTypes) {
    super();
    this.length = length;
    this.columnSnapshots = columnSnapshots;
    this.columnTypes = columnTypes;
  }
  @Override
  public Map<String, ColumnType> getColumnTypes() {
    return columnTypes;
  }
  @Override
  public TermValueList<?> getDictionary(String column) {
    throw new UnsupportedOperationException();
  }
  @Override
  public DocIdSet[] getInvertedIndex(String column) {
    return null;
  }
  @Override
  public ForwardIndex getForwardIndex(String column) {
    return columnSnapshots.get(column);
  }
  @Override
  public int getLength() {
    return length;
  }
}
