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
import com.browseengine.bobo.sort.DocComparatorSource;

/**
 * @author Praveen Neppalli Naga <pneppalli@linkedin.com>
 *
 */
public class SumFacetHandler extends FacetHandler<Serializable>{
  private final String name;

  public SumFacetHandler(String name) {
    super(name, new HashSet<String>());
    this.name = name;
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
    final String column = fspec.getProperties().get("column");
    return new FacetCountCollectorSource() {
      @Override
      public FacetCountCollector getFacetCountCollector(BoboIndexReader reader, int docBase) {
        return new SumFacetCollector(_name, (ZeusDataCache)reader.getFacetData(column), docBase, sel, fspec);
      }};
  }

  @Override
  public String[] getFieldValues(BoboIndexReader reader, int id) {
    return SumGroupByFacerHandler.EMPTY_STRING;
  }

  @Override
  public DocComparatorSource getDocComparatorSource() {
   throw new UnsupportedOperationException();
  }

}