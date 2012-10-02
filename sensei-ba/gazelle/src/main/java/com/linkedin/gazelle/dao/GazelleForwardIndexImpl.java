package com.linkedin.gazelle.dao;

import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.utils.ColumnMedata;
import com.linkedin.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.ForwardIndex;

public class GazelleForwardIndexImpl implements ForwardIndex {
  private CompressedIntArray _compressedIntArray;
  private final String _column;
  private TermValueList<?> _dictionary;
  private ColumnMedata _columnMetadata;

  public GazelleForwardIndexImpl(String column, CompressedIntArray compressedIntArray, TermValueList<?> dictionary, ColumnMedata columnMetadata) {
    _column = column;
    _compressedIntArray = compressedIntArray;
    _dictionary = dictionary;
    _columnMetadata = columnMetadata;
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

}
