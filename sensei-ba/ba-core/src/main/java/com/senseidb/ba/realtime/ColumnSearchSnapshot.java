package com.senseidb.ba.realtime;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.search.req.mapred.impl.dictionary.DictionaryNumberAccessor;

import it.unimi.dsi.fastutil.ints.IntList;

public interface ColumnSearchSnapshot extends DictionaryNumberAccessor, ForwardIndex {

  public abstract int size();

  public abstract String getStringValue(int unsortedDictId);

  public abstract Object getObject(int unsortedDictId);

  public int getIntValue(int unsortedDictId);

  public float getFloatValue(int unsortedDictId);

  public long getLongValue(int unsortedDictId);

  public double getDoubleValue(int unsortedDictId);

  public abstract short getShortValue(int unsortedDictId);

  public abstract IntList getPermutationArray();

  public void initForwardIndex(int[] forwardIndex, int forwardIndexSize, ColumnType columnType);

  int[] getForwardIndex();

  int getForwardIndexSize();

  public  void recycle();

}