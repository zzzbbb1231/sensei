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
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;

/**
 * @author Praveen Neppalli Naga <pneppalli@linkedin.com>
 * 
 */
public class SumGroupByFacetHandler extends FacetHandler<Serializable> {

  public static final String[] EMPTY_STRING = new String[0];

  public SumGroupByFacetHandler() {
    super("sumGroupBy", asSet());

  }

  public SumGroupByFacetHandler(String name) {
    super(name, asSet());

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

        ZeusDataCache groupByCache = (ZeusDataCache) reader.getFacetData(dimension);
        final ZeusDataCache sumOverDataCache = (ZeusDataCache) reader.getFacetData(metric);
        final TermNumberList valList = (TermNumberList) sumOverDataCache.getDictionary();
        if (groupByCache.getForwardIndex() instanceof SingleValueForwardIndex) {
          final SingleValueForwardIndex metricForwardIndex = (SingleValueForwardIndex) sumOverDataCache.getForwardIndex();
          final SingleValueForwardIndex dimensionForwardIndex = (SingleValueForwardIndex) groupByCache.getForwardIndex();
          if (valList instanceof TermIntList) {
            final TermIntList termIntList = (TermIntList) valList;
            return new GroupByFacetUtils.SingleValueIntFacetCountCollector(_name, groupByCache.getFakeCache(), docBase, sel, fspec, termIntList, dimensionForwardIndex, metricForwardIndex);
          } else if (valList instanceof TermLongList) {
            final TermLongList termLongList = (TermLongList) valList;
            return new GroupByFacetUtils.SingleValueLongFacetCountCollector(_name, groupByCache.getFakeCache(), docBase, sel, fspec, termLongList, dimensionForwardIndex, metricForwardIndex);
          } else {
            throw new UnsupportedOperationException(valList.getClass().toString());
          }
        } else if (groupByCache.getForwardIndex() instanceof MultiValueForwardIndex) {
          final SingleValueForwardIndex metricForwardIndex = (SingleValueForwardIndex) sumOverDataCache.getForwardIndex();
          final MultiValueForwardIndex dimensionForwardIndex = (MultiValueForwardIndex) groupByCache.getForwardIndex();
          if (valList instanceof TermIntList) {
            final TermIntList termIntList = (TermIntList) valList;
            return new GroupByFacetUtils.MultiValueIntFacetCountCollector(_name, groupByCache.getFakeCache(), docBase, sel, fspec, termIntList, dimensionForwardIndex, metricForwardIndex);
          } else if (valList instanceof TermLongList) {
            final TermLongList termLongList = (TermLongList) valList;
            return new GroupByFacetUtils.MultiValueLongFacetCountCollector(_name, groupByCache.getFakeCache(), docBase, sel, fspec, termLongList, dimensionForwardIndex, metricForwardIndex);
          } else {
            throw new UnsupportedOperationException(valList.getClass().toString());
          }
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