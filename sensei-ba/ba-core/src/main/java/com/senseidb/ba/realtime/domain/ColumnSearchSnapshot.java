package com.senseidb.ba.realtime.domain;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.search.req.mapred.impl.dictionary.DictionaryNumberAccessor;

import it.unimi.dsi.fastutil.ints.IntList;

public interface ColumnSearchSnapshot extends  ForwardIndex {

  public DictionarySnapshot getDictionarySnapshot();

  public void init(int[] forwardIndex, int forwardIndexSize, ColumnType columnType, DictionarySnapshot dictionarySnapshot);

  int[] getForwardIndex();

  int getForwardIndexSize();


}