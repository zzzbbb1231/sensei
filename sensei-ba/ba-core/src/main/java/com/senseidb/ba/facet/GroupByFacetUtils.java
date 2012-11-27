package com.senseidb.ba.facet;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.ba.gazelle.utils.multi.MultiFacetIterator;

public class GroupByFacetUtils {
  public static  class SingleValueLongFacetCountCollector extends DefaultFacetCountCollector {
    private final SingleValueForwardIndex dimensionForwardIndex;
    private final TermLongList termLongList;
    private final SingleValueForwardIndex metricForwardIndex;
    private final SingleValueRandomReader dimensionReader;
    private final SingleValueRandomReader metricReader;

    public SingleValueLongFacetCountCollector(String name, FacetDataCache dataCache, int docBase, BrowseSelection sel, FacetSpec ospec, TermLongList termLongList, SingleValueForwardIndex dimensionForwardIndex, SingleValueForwardIndex metricForwardIndex) {
      super(name, dataCache, docBase, sel, ospec);
      this.dimensionForwardIndex = dimensionForwardIndex;
      dimensionReader = dimensionForwardIndex.getReader();
      metricReader = metricForwardIndex.getReader();
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
      _count.add(dimensionReader.getValueIndex(docid),  _count.get(dimensionReader.getValueIndex(docid)) + (int)termLongList.getPrimitiveValue((metricReader.getValueIndex(docid))));
    }
  }
  public static  class MultiValueLongFacetCountCollector extends DefaultFacetCountCollector {
    private final MultiValueForwardIndex dimensionForwardIndex;
    private final TermLongList termLongList;
    private final SingleValueForwardIndex metricForwardIndex;
    private int[] buffer;
    private MultiFacetIterator iterator;
    private Object dimensionReader;
    private SingleValueRandomReader metricReader;

    public MultiValueLongFacetCountCollector(String name, FacetDataCache dataCache, int docBase, BrowseSelection sel, FacetSpec ospec, TermLongList termLongList, MultiValueForwardIndex dimensionForwardIndex, SingleValueForwardIndex metricForwardIndex) {
      super(name, dataCache, docBase, sel, ospec);
      this.dimensionForwardIndex = dimensionForwardIndex;
      this.termLongList = termLongList;
      this.metricForwardIndex = metricForwardIndex;
      metricReader = metricForwardIndex.getReader();
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
      int metricValue = (int)termLongList.getPrimitiveValue((metricReader.getValueIndex(docid)));
      while (++index < count) {
        int valueId = buffer[index];
        _count.add(valueId,  _count.get(valueId) + metricValue);
      }
      
    }
  }
  public static  class MultiValueIntFacetCountCollector extends DefaultFacetCountCollector {
    private final MultiValueForwardIndex dimensionForwardIndex;
    private final TermIntList termIntList;
    private final SingleValueForwardIndex metricForwardIndex;
    private int[] buffer;
    private MultiFacetIterator iterator;
    private SingleValueRandomReader metricReader;

    public MultiValueIntFacetCountCollector(String name, FacetDataCache dataCache, int docBase, BrowseSelection sel, FacetSpec ospec, TermIntList termIntList, MultiValueForwardIndex dimensionForwardIndex, SingleValueForwardIndex metricForwardIndex) {
      super(name, dataCache, docBase, sel, ospec);
      this.dimensionForwardIndex = dimensionForwardIndex;
      this.termIntList = termIntList;
      this.metricForwardIndex = metricForwardIndex;
      metricReader = metricForwardIndex.getReader();
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
      int metricValue = (int)termIntList.getPrimitiveValue((metricReader.getValueIndex(docid)));
      while (++index < count) {
        int valueId = buffer[index];
        _count.add(valueId,  _count.get(valueId) + metricValue);
      }
      
    }
  }
  public static class SingleValueIntFacetCountCollector extends DefaultFacetCountCollector {
    private final SingleValueForwardIndex metricForwardIndex;
    private final TermIntList termIntList;
    private final SingleValueForwardIndex dimensionForwardIndex;
    private SingleValueRandomReader dimensionReader;
    private SingleValueRandomReader metricReader;

    public SingleValueIntFacetCountCollector(String name, FacetDataCache dataCache, int docBase, BrowseSelection sel, FacetSpec ospec, TermIntList termIntList, SingleValueForwardIndex dimensionForwardIndex, SingleValueForwardIndex metricForwardIndex) {
      super(name, dataCache, docBase, sel, ospec);
      this.metricForwardIndex = metricForwardIndex;
      this.termIntList = termIntList;
      this.dimensionForwardIndex = dimensionForwardIndex;
      dimensionReader = dimensionForwardIndex.getReader();
      metricReader = metricForwardIndex.getReader();
    }

    @Override
    public void collectAll() {
      for (int docid=0; docid<dimensionForwardIndex.getLength(); docid++)
        collect(docid);
    }

    @Override
    public void collect(int docid) {
      _count.add(dimensionReader.getValueIndex(docid),  _count.get(dimensionReader.getValueIndex(docid)) + termIntList.getPrimitiveValue((metricReader.getValueIndex(docid))));
    }
  }
}
