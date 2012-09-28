package com.senseidb.ba;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.DocIdSet;

import com.browseengine.bobo.facets.data.TermValueList;

public class IndexSegmentImpl implements IndexSegment {
  Map<String, ForwardIndex> forwardIndexes = new HashMap<String, ForwardIndex>();
  Map<String, TermValueList> dictionaries = new HashMap<String, TermValueList>();
  Map<String, ColumnType> columnTypes = new HashMap<String, ColumnType>();
  
  int length;
  @Override
  public Map<String, ColumnType> getColumnTypes() {
    
    return columnTypes;
  }

  @Override
  public TermValueList<?> getDictionary(String column) {    
    return dictionaries.get(column);
  }

  @Override
  public DocIdSet[] getInvertedIndex(String column) {
    return null;
  }

  @Override
  public ForwardIndex getForwardIndex(String column) {
    return forwardIndexes.get(column);
  }

  @Override
  public int getLength() {
    return length;
  }

  public void setColumnTypes(Map<String, ColumnType> columnTypes) {
    this.columnTypes = columnTypes;
  }

public Map<String, ForwardIndex> getForwardIndexes() {
	return forwardIndexes;
}

public Map<String, TermValueList> getDictionaries() {
	return dictionaries;
}

public void setLength(int length) {
	this.length = length;
}
  
}
