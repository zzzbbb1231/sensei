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
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermNumberList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.senseidb.ba.SingleValueForwardIndex;
import com.senseidb.ba.plugins.ZeusFactoryFactory;

/**
 * @author Praveen Neppalli Naga <pneppalli@linkedin.com>
 *
 */
public class SumGroupByFacerHandler extends FacetHandler<Serializable>{
  public static final String[] EMPTY_STRING = new String[0];

  public SumGroupByFacerHandler() {
    super("sumGroupBy", asSet());
   
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
        
        ZeusDataCache groupByCache = (ZeusDataCache)reader.getFacetData(dimension);
        final ZeusDataCache sumOverDataCache = (ZeusDataCache) reader.getFacetData(metric);
        final TermNumberList valList =   (TermNumberList) sumOverDataCache.getDictionary();
        final SingleValueForwardIndex metricForwardIndex = (SingleValueForwardIndex)sumOverDataCache.getForwardIndex();
        final SingleValueForwardIndex dimensionForwardIndex = (SingleValueForwardIndex)groupByCache.getForwardIndex();
        if (valList instanceof TermIntList) {
         final  TermIntList termIntList = (TermIntList) valList;
        return new DefaultFacetCountCollector(_name, groupByCache.getFakeCache(), docBase, sel, fspec) {
          @Override
          public void collectAll() {
            for (int docid=0; docid<dimensionForwardIndex.getLength(); docid++)
              collect(docid);
          }

          @Override
          public void collect(int docid) {
            _count.add(dimensionForwardIndex.getValueIndex(docid),  _count.get(dimensionForwardIndex.getValueIndex(docid)) + termIntList.getPrimitiveValue((metricForwardIndex.getValueIndex(docid))));
          }
        };
        } else if (valList instanceof TermLongList) {
          final  TermLongList termLongList = (TermLongList) valList;
          return new DefaultFacetCountCollector(_name, groupByCache.getFakeCache(), docBase, sel, fspec) {
            @Override
            public void collectAll() {
              for (int docid=0; docid<dimensionForwardIndex.getLength(); docid++)
                collect(docid);
            }

            @Override
            public void collect(int docid) {
              _count.add(dimensionForwardIndex.getValueIndex(docid),  _count.get(dimensionForwardIndex.getValueIndex(docid)) + (int)termLongList.getPrimitiveValue((metricForwardIndex.getValueIndex(docid))));
            }
          };
        } else {
          throw new UnsupportedOperationException(valList.getClass().toString());
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