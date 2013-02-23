package com.senseidb.ba.realtime.domain;

import java.util.Map;

import org.apache.lucene.search.DocIdSet;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.InvertedIndexObject;
import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.SegmentAppendableIndex;

public class RealtimeSnapshotIndexSegment implements IndexSegment {
  private int length;
  private Map<String, ColumnSearchSnapshot> columnSnapshots;
  private Map<String, ColumnType> columnTypes;
  private SegmentAppendableIndex segmentAppendableIndex;
  private boolean isFull = false;
  
  
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
   return columnSnapshots.get(column).getDictionary();
  }
  @Override
  public InvertedIndexObject getInvertedIndex(String column) {
    return null;
  }
  @Override
  public ColumnSearchSnapshot getForwardIndex(String column) {
    return columnSnapshots.get(column);
  }
  @Override
  public int getLength() {
    return length;
  }
  public void setReferencedSegment(SegmentAppendableIndex segmentAppendableIndex) {
    this.segmentAppendableIndex = segmentAppendableIndex;
  }
  public SegmentAppendableIndex getReferencedSegment() {
    return segmentAppendableIndex;
  }
  public void recycle(ReusableIndexObjectsPool indexObjectsPool) {
    for (String column : columnSnapshots.keySet()) {
      ColumnSearchSnapshot columnSearchSnapshot = columnSnapshots.get(column);
      indexObjectsPool.recycle(columnSearchSnapshot.getDictionarySnapshot(), column);
      
    }
  }
  public boolean isFull() {
    return isFull;
  }
  public void setFull(boolean isFull) {
    this.isFull = isFull;
  }
  
  
}
