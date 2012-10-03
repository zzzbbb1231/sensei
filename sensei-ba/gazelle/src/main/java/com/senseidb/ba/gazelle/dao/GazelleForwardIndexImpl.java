package com.senseidb.ba.gazelle.dao;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.utils.GazelleColumnMetadata;
import com.senseidb.ba.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.ColumnMetadata;
import com.senseidb.ba.ForwardIndex;

public class GazelleForwardIndexImpl implements ForwardIndex {
  private CompressedIntArray _compressedIntArray;
  private final String _column;
  private TermValueList<?> _dictionary;
  private GazelleColumnMetadata _columnMetadata;
  private ColumnMetadata _metadata;
  public GazelleForwardIndexImpl(String column, CompressedIntArray compressedIntArray, TermValueList<?> dictionary, GazelleColumnMetadata columnMetadata) {
    _column = column;
    _compressedIntArray = compressedIntArray;
    _dictionary = dictionary;
    _columnMetadata = columnMetadata;
    _metadata = ColumnMetadata.transformToSelf(_columnMetadata);
  }

  @Override
  public int getLength() {
    return _compressedIntArray.getCapacity();
  }

  @Override
  public int getValueIndex(int docId) {
    return _compressedIntArray.readInt(docId);
  }

  @Override
  public int getFrequency(int valueId) {
    return 0;
  }

  @Override
  public TermValueList<?> getDictionary() {
    return _dictionary;
  }

  @Override
  public ColumnMetadata getColumnMetadata() {
    return _metadata;
  }
}
