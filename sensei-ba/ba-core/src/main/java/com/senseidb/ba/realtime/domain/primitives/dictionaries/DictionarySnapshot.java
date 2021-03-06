package com.senseidb.ba.realtime.domain.primitives.dictionaries;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.search.req.mapred.impl.dictionary.DictionaryNumberAccessor;

import it.unimi.dsi.fastutil.ints.IntList;

public interface DictionarySnapshot extends DictionaryNumberAccessor, Cloneable  {
  public abstract String getStringValue(int unsortedDictId);

  public abstract Object getObject(int unsortedDictId);

  public int getIntValue(int unsortedDictId);

  public float getFloatValue(int unsortedDictId);

  public long getLongValue(int unsortedDictId);

  public double getDoubleValue(int unsortedDictId);

  public abstract short getShortValue(int unsortedDictId);

  public abstract IntList getDictPermutationArray();

  public  void recycle();
  
  public TermValueList produceDictionary();
  public int sortedIndexOf(String value);
  public IntList getInvPermutationArray();
  int size();
  public DictionaryResurrectingMarker getResurrectingMarker();
  public void setDictionaryResurrectingMarker(DictionaryResurrectingMarker dictionaryResurrectingMarker);
  
}
