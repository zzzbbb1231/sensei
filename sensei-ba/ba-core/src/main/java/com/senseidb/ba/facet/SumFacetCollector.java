package com.senseidb.ba.facet;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermNumberList;
import com.browseengine.bobo.facets.data.TermShortList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;

/**
 * @author Praveen Neppalli Naga <pneppalli@linkedin.com>
 *
 */
@SuppressWarnings("rawtypes")
public class SumFacetCollector extends DefaultFacetCountCollector {

  private final ZeusDataCache dataCache;
  private TermNumberList<?> valArray;
  private SingleValueForwardIndex forwardIndex;
  private SingleValueRandomReader reader;

  public SumFacetCollector(String name,ZeusDataCache dataCache,int docBase,
                           BrowseSelection sel,FacetSpec ospec) {
    super(name, createFakeFacetDataCache(dataCache), docBase, sel, ospec);
    this.dataCache = dataCache;
    valArray =  (TermNumberList<?>) dataCache.getDictionary();
     forwardIndex = (SingleValueForwardIndex) dataCache.getForwardIndex();
     reader = forwardIndex.getReader();
  }
  public static TermValueList<String> createFakeTermValueList() {
    TermStringList list = new TermStringList();
    list.add(null);
    list.add("sum");
    return list;
  }

  public static FacetDataCache createFakeFacetDataCache(ZeusDataCache dataCache) {
    FacetDataCache newDataCache = new FacetDataCache<String>();
    newDataCache.valArray = createFakeTermValueList();
    newDataCache.minIDs = new int[] {-1, 0};
    newDataCache.maxIDs = new int[] {-1, dataCache.getForwardIndex().getLength()};
    newDataCache.freqs = new int[] {0, dataCache.getForwardIndex().getLength()};
    return newDataCache;
  }

  @Override
  public void collect(int docid) {
    if (valArray instanceof TermIntList) {
      _count.add(1, _count.get(1) + ((TermIntList) valArray).getPrimitiveValue(reader.getValueIndex(docid)));
    } else if (valArray instanceof TermShortList) {
      _count.add(1, _count.get(1) + ((TermShortList) valArray).getPrimitiveValue(reader.getValueIndex(docid)));
    } else if (valArray instanceof TermLongList) {
      _count.add(1, _count.get(1) + (int)((TermLongList) valArray).getPrimitiveValue(reader.getValueIndex(docid)));
    }
    else {
      _count.add(1, _count.get(1) + (int)((TermNumberList) valArray).getDoubleValue(reader.getValueIndex(docid)));
    }
  }

  @Override
  public void collectAll() {
    for (int i = 0; i <  forwardIndex.getLength(); i++) {
      collect(i);
    }
  }
}