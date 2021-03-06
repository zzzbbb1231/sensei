package com.senseidb.ba.realtime.domain;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;

public interface ColumnSearchSnapshot<T> extends  ForwardIndex {

  public DictionarySnapshot getDictionarySnapshot();

  public void init(T forwardIndex, int forwardIndexSize, ColumnType columnType, DictionarySnapshot dictionarySnapshot);

  T getForwardIndex();

  int getForwardIndexSize();

  boolean isSingleValue();
  public void setForwardIndexSize(int forwardIndexSize);
}