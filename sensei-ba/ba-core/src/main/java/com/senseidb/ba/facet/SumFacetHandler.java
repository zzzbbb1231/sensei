package com.senseidb.ba.facet;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.browseengine.bobo.sort.DocComparatorSource;

/**
 * @author Praveen Neppalli Naga <pneppalli@linkedin.com>
 *
 */
public class SumFacetHandler extends FacetHandler<Serializable>{
  public static final class EmptyCountCollector extends DefaultFacetCountCollector {
    public EmptyCountCollector(String name, FacetDataCache dataCache, int docBase, BrowseSelection sel, FacetSpec ospec) {
      super(name, dataCache, docBase, sel, ospec);
    }

    @Override
    public void collectAll() {            
    }

    @Override
    public void collect(int docid) {           
    }
  }

  private final String name;
  private String column;
  public SumFacetHandler(String name) {
    super(name, new HashSet<String>());
    this.name = name;
  }
  public SumFacetHandler(String name, String column) {
    super(name, new HashSet<String>());
    this.name = name;
    this.column = column;
  }


  @Override
  public Serializable load(BoboIndexReader reader) throws IOException {
    return FacetHandler.FacetDataNone.instance;
  }

  @Override
  public RandomAccessFilter buildRandomAccessFilter(String value, Properties selectionProperty) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(final BrowseSelection sel, final FacetSpec fspec) {
    String currentColumnStr = null;
    if (fspec.getProperties().size() > 0) {
      currentColumnStr = fspec.getProperties().get("column");
      if (currentColumnStr == null) {
        currentColumnStr = fspec.getProperties().get("metric");
      }  
      
    }
    if (currentColumnStr == null) {
      currentColumnStr = column;
    }
    final String currentColumn = currentColumnStr;
    return new FacetCountCollectorSource() {
      @Override
      public FacetCountCollector getFacetCountCollector(BoboIndexReader reader, final int docBase) {
        ZeusDataCache facetData = (ZeusDataCache)reader.getFacetData(currentColumn);
        if (facetData == null) {
        return new EmptyCountCollector(_name, SumFacetCollector.createFakeFacetDataCache(), docBase, sel, fspec);
        }
        return new SumFacetCollector(_name, facetData, docBase, sel, fspec);
      }};
  }

  @Override
  public String[] getFieldValues(BoboIndexReader reader, int id) {
    return SumGroupByFacetHandler.EMPTY_STRING;
  }

  @Override
  public DocComparatorSource getDocComparatorSource() {
   throw new UnsupportedOperationException();
  }

}