package com.senseidb.ba.realtime.domain;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.realtime.domain.multi.MultiArray;

public interface MultiColumnSearchSnapshot extends  ForwardIndex {

  public DictionarySnapshot getDictionarySnapshot();

  public void init(MultiArray forwardIndex, int forwardIndexSize, ColumnType columnType, DictionarySnapshot dictionarySnapshot);

  MultiArray getForwardIndex();

  int getForwardIndexSize();


}
