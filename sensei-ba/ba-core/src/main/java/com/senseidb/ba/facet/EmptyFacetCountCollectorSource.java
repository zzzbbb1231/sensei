package com.senseidb.ba.facet;

import java.util.Collections;
import java.util.List;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.FacetIterator;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.util.BigIntArray;
import com.browseengine.bobo.util.BigSegmentedArray;

public  class EmptyFacetCountCollectorSource implements FacetCountCollector {
  private static final BigIntArray EMPTY_INT_ARRAY = new BigIntArray(0);
  private final String name;
  public EmptyFacetCountCollectorSource(String _name) {
    name = _name;
  }
  @Override
  public FacetIterator iterator() {
    return new FacetIterator() {
      @Override
      public void remove() { 
      }
      @Override
      public boolean hasNext() {
        return false;
      }
      @Override
      public Comparable next(int minHits) {
        // TODO Auto-generated method stub
        return null;
      }
      @Override
      public Comparable next() {
        return null;
      }
      
      @Override
      public String format(Object val) {
        return null;
      }
    };
  }
  @Override
  public List<BrowseFacet> getFacets() {
    return Collections.EMPTY_LIST;
  }
  @Override
  public int getFacetHitsCount(Object value) {
    return 0;
  }
  @Override
  public BrowseFacet getFacet(String value) {
    return null;
  }
  @Override
  public void close() {
  }
  @Override
  public String getName() {
    return name;
  }
  @Override
  public BigSegmentedArray getCountDistribution() {
    return EMPTY_INT_ARRAY;
  }
  @Override
  public void collectAll() {
  }
  @Override
  public void collect(int docid) {
  }
}