package com.senseidb.ba.facet;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.utils.multi.MultiFacetIterator;

public class GroupByFacetUtils {
  public static  class SingleValueLongFacetCountCollector extends DefaultFacetCountCollector {
    private final SingleValueForwardIndex dimensionForwardIndex;
    private final TermLongList termLongList;
    private final SingleValueForwardIndex metricForwardIndex;

    public SingleValueLongFacetCountCollector(String name, FacetDataCache dataCache, int docBase, BrowseSelection sel, FacetSpec ospec, TermLongList termLongList, SingleValueForwardIndex dimensionForwardIndex, SingleValueForwardIndex metricForwardIndex) {
      super(name, dataCache, docBase, sel, ospec);
      this.dimensionForwardIndex = dimensionForwardIndex;
      this.termLongList = termLongList;
      this.metricForwardIndex = metricForwardIndex;
    }

    @Override
    public void collectAll() {
      for (int docid=0; docid<dimensionForwardIndex.getLength(); docid++)
        collect(docid);
    }

    @Override
    public void collect(int docid) {
      _count.add(dimensionForwardIndex.getValueIndex(docid),  _count.get(dimensionForwardIndex.getValueIndex(docid)) + (int)termLongList.getPrimitiveValue((metricForwardIndex.getValueIndex(docid))));
    }
  }
  public static  class MultiValueLongFacetCountCollector extends DefaultFacetCountCollector {
    private final MultiValueForwardIndex dimensionForwardIndex;
    private final TermLongList termLongList;
    private final SingleValueForwardIndex metricForwardIndex;
    private int[] buffer;
    private MultiFacetIterator iterator;

    public MultiValueLongFacetCountCollector(String name, FacetDataCache dataCache, int docBase, BrowseSelection sel, FacetSpec ospec, TermLongList termLongList, MultiValueForwardIndex dimensionForwardIndex, SingleValueForwardIndex metricForwardIndex) {
      super(name, dataCache, docBase, sel, ospec);
      this.dimensionForwardIndex = dimensionForwardIndex;
      this.termLongList = termLongList;
      this.metricForwardIndex = metricForwardIndex;
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
      while (++index < count) {
        int valueId = buffer[index];
        _count.add(valueId,  _count.get(valueId) + (int)termLongList.getPrimitiveValue((metricForwardIndex.getValueIndex(docid))));
      }
      
    }
  }
  public static  class MultiValueIntFacetCountCollector extends DefaultFacetCountCollector {
    private final MultiValueForwardIndex dimensionForwardIndex;
    private final TermIntList termIntList;
    private final SingleValueForwardIndex metricForwardIndex;
    private int[] buffer;
    private MultiFacetIterator iterator;

    public MultiValueIntFacetCountCollector(String name, FacetDataCache dataCache, int docBase, BrowseSelection sel, FacetSpec ospec, TermIntList termIntList, MultiValueForwardIndex dimensionForwardIndex, SingleValueForwardIndex metricForwardIndex) {
      super(name, dataCache, docBase, sel, ospec);
      this.dimensionForwardIndex = dimensionForwardIndex;
      this.termIntList = termIntList;
      this.metricForwardIndex = metricForwardIndex;
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
      while (++index < count) {
        int valueId = buffer[index];
        _count.add(valueId,  _count.get(valueId) + (int)termIntList.getPrimitiveValue((metricForwardIndex.getValueIndex(docid))));
      }
      
    }
  }
  public static class SingleValueIntFacetCountCollector extends DefaultFacetCountCollector {
    private final SingleValueForwardIndex metricForwardIndex;
    private final TermIntList termIntList;
    private final SingleValueForwardIndex dimensionForwardIndex;

    public SingleValueIntFacetCountCollector(String name, FacetDataCache dataCache, int docBase, BrowseSelection sel, FacetSpec ospec, TermIntList termIntList, SingleValueForwardIndex dimensionForwardIndex, SingleValueForwardIndex metricForwardIndex) {
      super(name, dataCache, docBase, sel, ospec);
      this.metricForwardIndex = metricForwardIndex;
      this.termIntList = termIntList;
      this.dimensionForwardIndex = dimensionForwardIndex;
    }

    @Override
    public void collectAll() {
      for (int docid=0; docid<dimensionForwardIndex.getLength(); docid++)
        collect(docid);
    }

    @Override
    public void collect(int docid) {
      _count.add(dimensionForwardIndex.getValueIndex(docid),  _count.get(dimensionForwardIndex.getValueIndex(docid)) + termIntList.getPrimitiveValue((metricForwardIndex.getValueIndex(docid))));
    }
  }
}
