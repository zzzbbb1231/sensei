package com.senseidb.ba.facet;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.browseengine.bobo.util.LazyBigIntArray;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.ba.gazelle.utils.multi.MultiFacetIterator;
import com.senseidb.search.req.mapred.impl.dictionary.DictionaryNumberAccessor;

public class GroupByFacetUtils {

  public static  class MultiValueFacetCountCollector extends DefaultFacetCountCollector {
    private final MultiValueForwardIndex dimensionForwardIndex;
    private int[] buffer;
    private MultiFacetIterator iterator;
    private SingleValueRandomReader metricReader;
    private final DictionaryNumberAccessor numberAccessor;

    @SuppressWarnings("rawtypes")
    public MultiValueFacetCountCollector(String name, FacetDataCache dataCache, int docBase, BrowseSelection sel, FacetSpec ospec, DictionaryNumberAccessor numberAccessor, MultiValueForwardIndex dimensionForwardIndex, SingleValueRandomReader metricReader, boolean isRealtime) {
      super(name, dataCache, docBase, sel, ospec);
      if (isRealtime) {
        _countlength = dataCache.valArray.size();
        if (_countlength <= 3096)
        {
          _count = new LazyBigIntArray(_countlength);
        } else
        {
          _count = intarraymgr.get(_countlength);
          intarraylist.add(_count);
        }
      }
      this.dimensionForwardIndex = dimensionForwardIndex;
      this.numberAccessor = numberAccessor;
      this.metricReader = metricReader;
      iterator = dimensionForwardIndex.getIterator();
      buffer = new int[dimensionForwardIndex.getMaxNumValuesPerDoc()];
    }

    @Override
    public void collectAll() {
      for (int docid=0; docid<dimensionForwardIndex.getLength(); docid++)
        collect(docid);
    }

    @Override
    public void collect(int docid) {
      iterator.advance(docid);
      int count = iterator.readValues(buffer);
      int index = -1;
      int metricValue = numberAccessor.getIntValue(metricReader.getValueIndex(docid));
      while (++index < count) {
        int valueId = buffer[index];
        _count.add(valueId,  _count.get(valueId) + metricValue);
      }
      
    }
  }
  public static class SingleValueFacetCountCollector extends DefaultFacetCountCollector {
    private final SingleValueForwardIndex dimensionForwardIndex;
    private SingleValueRandomReader dimensionReader;
    private SingleValueRandomReader metricReader;
    private final DictionaryNumberAccessor numberAccessor;

    @SuppressWarnings("rawtypes")
    public SingleValueFacetCountCollector(String name, FacetDataCache dataCache, int docBase, BrowseSelection sel, FacetSpec ospec, DictionaryNumberAccessor numberAccessor, SingleValueForwardIndex dimensionForwardIndex, SingleValueRandomReader metricReader, boolean isRealtime) {
      super(name, dataCache, docBase, sel, ospec);
      this.numberAccessor = numberAccessor;
      if (isRealtime) {
        _countlength = dataCache.valArray.size();
        if (_countlength <= 3096)
        {
          _count = new LazyBigIntArray(_countlength);
        } else
        {
          _count = intarraymgr.get(_countlength);
          intarraylist.add(_count);
        }
      }
      this.metricReader = metricReader;
     
      this.dimensionForwardIndex = dimensionForwardIndex;
      dimensionReader = dimensionForwardIndex.getReader();
    }

    @Override
    public void collectAll() {
      for (int docid=0; docid<dimensionForwardIndex.getLength(); docid++)
        collect(docid);
    }

    @Override
    public void collect(int docid) {
      _count.add(dimensionReader.getValueIndex(docid),   _count.get(dimensionReader.getValueIndex(docid)) + numberAccessor.getIntValue((metricReader.getValueIndex(docid))));
    }
  }
}
