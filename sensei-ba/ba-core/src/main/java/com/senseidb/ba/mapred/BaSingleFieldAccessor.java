package com.senseidb.ba.mapred;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.util.BigSegmentedArray;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.search.req.mapred.SingleFieldAccessor;
import com.senseidb.search.req.mapred.impl.dictionary.AccessorFactory;
import com.senseidb.search.req.mapred.impl.dictionary.DictionaryNumberAccessor;

public class BaSingleFieldAccessor implements SingleFieldAccessor {
  private ForwardIndex forwardIndex;
  private DictionaryNumberAccessor dictionaryNumberAccessor;
  private TermValueList valArray;
  private final FacetHandler facetHandler;
  private final BoboIndexReader reader;
  private SingleValueRandomReader singleValueReader;

  @SuppressWarnings("rawtypes")
  public BaSingleFieldAccessor(ForwardIndex forwardIndex,  FacetHandler facetHandler, BoboIndexReader reader) {
      this.facetHandler = facetHandler;
      this.reader = reader;
      this.forwardIndex = forwardIndex;
      //true for realtime
      if (forwardIndex.getDictionary() instanceof DictionaryNumberAccessor) {
        dictionaryNumberAccessor = (DictionaryNumberAccessor) forwardIndex.getDictionary();
      } else {
        dictionaryNumberAccessor = AccessorFactory.get(forwardIndex.getDictionary());
      }
      valArray = forwardIndex.getDictionary();
      if (forwardIndex instanceof SingleValueForwardIndex) {
        singleValueReader = ((SingleValueForwardIndex)forwardIndex).getReader();
      }
  }
  @Override
  public Object get(int docId) {
      return valArray.getRawValue(singleValueReader.getValueIndex(docId));
  }

  @Override
  public String getString(int docId) {
      return valArray.get(singleValueReader.getValueIndex(docId));
  }

  @Override
  public long getLong(int docId) {
      return dictionaryNumberAccessor.getLongValue(singleValueReader.getValueIndex(docId));
  }

  @Override
  public double getDouble(int docId) {
      return dictionaryNumberAccessor.getDoubleValue(singleValueReader.getValueIndex(docId));
  }

  @Override
  public short getShort(int docId) {
      return (short) dictionaryNumberAccessor.getIntValue(singleValueReader.getValueIndex(docId));
  }

  @Override
  public int getInteger(int docId) {
      return dictionaryNumberAccessor.getIntValue(singleValueReader.getValueIndex(docId));
  }

  @Override
  public float getFloat(int docId) {
      return (short) dictionaryNumberAccessor.getFloatValue(singleValueReader.getValueIndex(docId));
  }

  @Override
  public Object[] getArray(int docId) {
      return facetHandler.getRawFieldValues(reader, docId);
  }
  @Override
  public int getDictionaryId(int docId) {
    return singleValueReader.getValueIndex(docId);
  }
}
