package com.senseidb.ba.gazelle.impl;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.ColumnMetadata;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.ForwardIndex;

public class GazelleForwardIndexImpl implements ForwardIndex {
  private CompressedIntArray compressedIntArray;
  private final String _column;
  private TermValueList<?> _dictionary;
  private ColumnMetadata _columnMetadata;
  public GazelleForwardIndexImpl(String column, CompressedIntArray compressedIntArray, TermValueList<?> dictionary, ColumnMetadata columnMetadata) {
    _column = column;
    this.compressedIntArray = compressedIntArray;
    _dictionary = dictionary;
    _columnMetadata = columnMetadata;
  }

  public CompressedIntArray getCompressedIntArray() {
    return compressedIntArray;
  }

  @Override
  public int getLength() {
    return compressedIntArray.getCapacity();
  }

  @Override
  public int getValueIndex(int docId) {
    return compressedIntArray.readInt(docId);
  }

  @Override
  public int getFrequency(int valueId) {
    return 0;
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
