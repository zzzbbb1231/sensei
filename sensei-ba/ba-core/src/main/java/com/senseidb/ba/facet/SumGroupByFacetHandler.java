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
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermNumberList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.search.req.mapred.impl.dictionary.AccessorFactory;
import com.senseidb.search.req.mapred.impl.dictionary.DictionaryNumberAccessor;


public class SumGroupByFacetHandler extends FacetHandler<Serializable> {

  public static final String[] EMPTY_STRING = new String[0];
  private String metric;
  private String dimension;

  public SumGroupByFacetHandler() {
    super("sumGroupBy", asSet());

  }

  public SumGroupByFacetHandler(String name) {
    super(name, asSet());

  }

  public SumGroupByFacetHandler(String name, String dimension, String metric ) {
    super(name, asSet());
    this.dimension = dimension;
    this.metric = metric;

  }
  public static Set<String> asSet(String... vals) {
    Set<String> ret = new HashSet<String>(vals.length);
    for (String str : vals) {
      ret.add(str);
    }
    return ret;
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
    return new FacetCountCollectorSource() {
      @Override
      public FacetCountCollector getFacetCountCollector(BoboIndexReader reader, int docBase) {
        String dimension = fspec.getProperties().get("dimension");
        String metric = fspec.getProperties().get("metric");
       if (dimension == null) {
         dimension = SumGroupByFacetHandler.this.dimension;
       }
       if (metric == null) {
         metric = SumGroupByFacetHandler.this.metric;
       }
        ZeusDataCache groupByCache = (ZeusDataCache) reader.getFacetData(dimension);

        final ZeusDataCache sumOverDataCache = (ZeusDataCache) reader.getFacetData(metric);
        final TermValueList valList = (TermValueList) sumOverDataCache.getDictionary();
        boolean isRealtime = false;
        DictionaryNumberAccessor numberAccessor = null; 
        if (valList instanceof DictionaryNumberAccessor) {
          //in case of realtime indexing
          isRealtime = true;
          numberAccessor = (DictionaryNumberAccessor) valList;
        } else {
          numberAccessor = AccessorFactory.get(valList);
        }
        final SingleValueForwardIndex metricForwardIndex = (SingleValueForwardIndex) sumOverDataCache.getForwardIndex();
        if (groupByCache.getForwardIndex() instanceof SingleValueForwardIndex) {
          final SingleValueForwardIndex dimensionForwardIndex = (SingleValueForwardIndex) groupByCache.getForwardIndex();
            return new GroupByFacetUtils.SingleValueFacetCountCollector(_name, groupByCache.getFakeCache(), docBase, sel, fspec, numberAccessor, dimensionForwardIndex, metricForwardIndex, isRealtime);
        } else if (groupByCache.getForwardIndex() instanceof MultiValueForwardIndex) {
         
          final MultiValueForwardIndex dimensionForwardIndex = (MultiValueForwardIndex) groupByCache.getForwardIndex();
            return new GroupByFacetUtils.MultiValueFacetCountCollector(_name, groupByCache.getFakeCache(), docBase, sel, fspec, numberAccessor, dimensionForwardIndex, metricForwardIndex, isRealtime);
          
        } else {
          throw new UnsupportedOperationException("Either single or multi column value are supported");
        }

      }
    };
  }

  @Override
  public String[] getFieldValues(BoboIndexReader reader, int id) {
    return EMPTY_STRING;
  }

  @Override
  public DocComparatorSource getDocComparatorSource() {
    throw new UnsupportedOperationException();
  }
}