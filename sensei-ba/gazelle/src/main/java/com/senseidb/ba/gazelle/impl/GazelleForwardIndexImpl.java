package com.senseidb.ba.gazelle.impl;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.MetadataAware;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.utils.IntArray;

public class GazelleForwardIndexImpl implements SingleValueForwardIndex, MetadataAware {
  private IntArray compressedIntArray;
  private final String _column;
  private TermValueList<?> _dictionary;
  private ColumnMetadata _columnMetadata;
  public GazelleForwardIndexImpl(String column, IntArray compressedIntArray, TermValueList<?> dictionary, ColumnMetadata columnMetadata) {
    _column = column;
    this.compressedIntArray = compressedIntArray;
    _dictionary = dictionary;
    _columnMetadata = columnMetadata;
  }

  public IntArray getCompressedIntArray() {
    return compressedIntArray;
  }

  @Override
  public int getLength() {
    return compressedIntArray.size();
  }

  @Override
  public int getValueIndex(int docId) {
    return compressedIntArray.getInt(docId);
  }


  @Override
  public TermValueList<?> getDictionary() {
    return _dictionary;
  }

 
  public ColumnMetadata getColumnMetadata() {
    return _columnMetadata;
  }

  @Override
  public ColumnType getColumnType() {
    return _columnMetadata.getColumnType();
  }
}
