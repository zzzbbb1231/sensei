package com.senseidb.ba.facet;

import org.apache.lucene.search.DocIdSet;

import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;

public class ZeusDataCache {
  private FacetDataCache fakeCache;
  private DocIdSet[] invertedIndexes;
  private ForwardIndex forwardIndex;
  private TermValueList<?> dictionary;
  public ZeusDataCache(ForwardIndex forwardIndex, DocIdSet[] invertedIndexes) {
    this.forwardIndex = forwardIndex;
    this.invertedIndexes = invertedIndexes;
    dictionary = forwardIndex.getDictionary();
    if ((forwardIndex instanceof ColumnSearchSnapshot)) {
      fakeCache = createRealtimeFakeFacetDataCache(forwardIndex);
    } else {
      fakeCache = createFakeFacetDataCache(forwardIndex);
    }
  }
  


  public boolean invertedIndexPresent(int dictionaryIndex) {
    return invertedIndexes != null && dictionaryIndex < invertedIndexes.length && invertedIndexes[dictionaryIndex] != null;
  }
  public static FacetDataCache createFakeFacetDataCache(ForwardIndex forwardIndex) {
    FacetDataCache newDataCache = new FacetDataCache<String>();
    newDataCache.valArray = forwardIndex.getDictionary(); 
    newDataCache.freqs =  new int[forwardIndex.getDictionary().size()];
    for (int i = 0 ; i < forwardIndex.getDictionary().size(); i++) {
      newDataCache.freqs[i] = 1;
    }    
    return newDataCache;
  }
  public static final int[] STATIC_FREQS = new int[2];
  static  {
    for (int i = 0 ; i < STATIC_FREQS.length; i++) {
      STATIC_FREQS[i] = 1;
    }    
  }
  private FacetDataCache createRealtimeFakeFacetDataCache(ForwardIndex forwardIndex2) {
    FacetDataCache newDataCache = new FacetDataCache<String>();
    newDataCache.valArray = forwardIndex.getDictionary(); 
    newDataCache.freqs =  STATIC_FREQS;    
    return newDataCache;
  }
  public FacetDataCache getFakeCache() {
    return fakeCache;
  }
  public void setFakeCache(FacetDataCache fakeCache) {
    this.fakeCache = fakeCache;
  }
  public DocIdSet[] getInvertedIndexes() {
    return invertedIndexes;
  }
  public void setInvertedIndexes(DocIdSet[] invertedIndexes) {
    this.invertedIndexes = invertedIndexes;
  }

  public ForwardIndex getForwardIndex() {
    return forwardIndex;
  }

  public void setForwardIndex(ForwardIndex forwardIndex) {
    this.forwardIndex = forwardIndex;
  }

  public TermValueList<?> getDictionary() {
    return dictionary;
  }

  public void setDictionary(TermValueList<?> dictionary) {
    this.dictionary = dictionary;
  }
  
}
