package com.senseidb.ba.facet;

import java.io.IOException;
import java.util.Properties;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.ScoreDoc;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.filter.FacetRangeFilter;
import com.browseengine.bobo.facets.filter.RandomAccessFilter;
import com.browseengine.bobo.sort.DocComparator;
import com.browseengine.bobo.sort.DocComparatorSource;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SortedForwardIndex;
import com.senseidb.ba.util.QueryUtils;

public class BaFacetHandler extends FacetHandler<ZeusDataCache> {
  private final String bootsrapFacetHandlerName;
  private final String columnName;

  public BaFacetHandler(String name, String columnName, String bootsrapFacetHandlerName) {
    super(name);
    this.columnName = columnName;
    this.bootsrapFacetHandlerName = bootsrapFacetHandlerName;

  }

  @Override
  public ZeusDataCache load(BoboIndexReader reader) {
    if (reader.getFacetData(columnName) != null) {
      return (ZeusDataCache) reader.getFacetData(columnName);
    }
    IndexSegment offlineSegment =
        (IndexSegment) reader.getFacetData(bootsrapFacetHandlerName);
    return new ZeusDataCache(offlineSegment.getForwardIndex(columnName), offlineSegment.getInvertedIndex(columnName));

  }

  private RandomAccessFilter getDefaultRandomAccessFilter(final String value, Properties selectionProperty) {
    return new RandomAccessFilter() {
      @Override
      public double getFacetSelectivity(BoboIndexReader reader) {
        final ZeusDataCache zeusDataCache = BaFacetHandler.this.load(reader);
        final int index = zeusDataCache.getDictionary().indexOf(value);
        if (index < 0)
          return 0.0;
        return ((double) zeusDataCache.getForwardIndex().getDictionary().size())
            / zeusDataCache.getForwardIndex().getLength();
      }

      @Override
      public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader) throws IOException {
        final ZeusDataCache zeusDataCache = BaFacetHandler.this.load(reader);
        final int index = zeusDataCache.getDictionary().indexOf(value);
        if (index < 0) {
          return EmptyDocIdSet.getInstance();
        }
        if (zeusDataCache.getForwardIndex() instanceof SortedForwardIndex) {
          return new SortedFacetUtils.SortedForwardDocIdSet((SortedForwardIndex) zeusDataCache.getForwardIndex(), index);
        }
        // Go by inverted index path
        if (zeusDataCache.invertedIndexPresent(index)) {
          final DocIdSet invertedIndex =
              zeusDataCache.getInvertedIndexes()[index];
          return new FacetUtils.InvertedIndexDocIdSet(zeusDataCache, invertedIndex, index);
        } else if (zeusDataCache.getForwardIndex() instanceof SingleValueForwardIndex) {
          return new FacetUtils.ForwardDocIdSet(((SingleValueForwardIndex) zeusDataCache.getForwardIndex()), index);
        } else if (zeusDataCache.getForwardIndex() instanceof MultiValueForwardIndex) {
          return new MultiFacetUtils.MultiForwardDocIdSet(((MultiValueForwardIndex) zeusDataCache.getForwardIndex()), index);
        }

        throw new UnsupportedOperationException();
      }
    };
  }

  private RandomAccessFilter getRangeRandomAccessFilter(final String value, final String[] values, Properties selectionProperty) {
    return new RandomAccessFilter() {
      @Override
      public double getFacetSelectivity(BoboIndexReader reader) {
        final ZeusDataCache zeusDataCache = BaFacetHandler.this.load(reader);
        return ((double) zeusDataCache.getForwardIndex().getDictionary().size())
            / zeusDataCache.getForwardIndex().getLength();
      }

      @Override
      public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader) throws IOException {
        final ZeusDataCache zeusDataCache = BaFacetHandler.this.load(reader);
        final int startIndex;
        final int endIndex;
        
        int sIndex = zeusDataCache.getDictionary().indexOf(values[0]);
        int eIndex = zeusDataCache.getDictionary().indexOf(values[1]);
        if (sIndex < 0) {
          sIndex = -(sIndex + 1);
        }
        if (eIndex < 0) {
          eIndex = -(eIndex + 1);
          eIndex = Math.max(0, eIndex - 1);
        }
        if (QueryUtils.isExclusiveRangeQuery(value)) {
          sIndex += 1;
          eIndex -= 1;
        }
        
        startIndex = sIndex;
        endIndex = eIndex;
        if (zeusDataCache.getForwardIndex() instanceof SingleValueForwardIndex) {
          return new FacetUtils.RangeForwardDocIdSet(((SingleValueForwardIndex) zeusDataCache.getForwardIndex()), startIndex, endIndex);
        } else if (zeusDataCache.getForwardIndex() instanceof MultiValueForwardIndex) {
          return new MultiFacetUtils.RangeMultiForwardDocIdSet(((MultiValueForwardIndex) zeusDataCache.getForwardIndex()), startIndex, endIndex);
        } else if (zeusDataCache.getForwardIndex() instanceof SortedForwardIndex) {
          return new SortedFacetUtils.RangeSortedForwardDocIdSet((SortedForwardIndex) zeusDataCache.getForwardIndex(), startIndex, endIndex);
        }
        return null;
      }
    };

  }

  @Override
  public RandomAccessFilter buildRandomAccessFilter(final String value, Properties selectionProperty) throws IOException {
    String[] vals = new String[2];
    try {
      if (QueryUtils.isRangeQuery(value)) {
        vals = FacetRangeFilter.getRangeStrings(value);
      }
      if (vals != null & vals[0] != null & vals[1] != null) {
        return getRangeRandomAccessFilter(value, vals, selectionProperty);
      } else {
        return getDefaultRandomAccessFilter(value, selectionProperty);
      }
    } catch (RuntimeException e) {
      throw new RuntimeException("error parsing range string");
    }
  }

  @Override
  public FacetCountCollectorSource getFacetCountCollectorSource(final BrowseSelection sel, final FacetSpec fspec) {

    return new FacetCountCollectorSource() {

      @Override
      public FacetCountCollector getFacetCountCollector(BoboIndexReader reader, int docBase) {
        ZeusDataCache dataCache = load(reader);
        final ForwardIndex forwardIndex = dataCache.getForwardIndex();

        final FacetDataCache fakeCache = dataCache.getFakeCache();
        if (forwardIndex instanceof SortedForwardIndex) {
          return new SortedFacetUtils.SortedFacetCountCollector((SortedForwardIndex) forwardIndex, getName(), fakeCache, docBase, sel, fspec);
        }
        if (forwardIndex instanceof SingleValueForwardIndex) {
          return new FacetUtils.ForwardIndexCountCollector(getName(), dataCache.getFakeCache(), (SingleValueForwardIndex) forwardIndex, docBase, sel, fspec);
        }
        if (forwardIndex instanceof MultiValueForwardIndex) {
          return new MultiFacetUtils.MultiForwardIndexCountCollector(getName(), dataCache.getFakeCache(), (MultiValueForwardIndex) forwardIndex, docBase, sel, fspec);
        }
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public String[] getFieldValues(BoboIndexReader reader, int id) {
    ForwardIndex forwardIndex = load(reader).getForwardIndex();
    if (forwardIndex instanceof SortedForwardIndex) {
      int dictionaryValueId =
          SortedFacetUtils.getDictionaryValueId((SortedForwardIndex) forwardIndex, id);
      if (dictionaryValueId < 0) {
        return new String[0];
      } else {
        return new String[] { forwardIndex.getDictionary().get(dictionaryValueId) };
      }
    }
    if (forwardIndex instanceof SingleValueForwardIndex) {
      SingleValueForwardIndex forwardIndex2 =
          (SingleValueForwardIndex) forwardIndex;
      return new String[] { forwardIndex2.getDictionary().get(forwardIndex2.getValueIndex(id)) };
    }
    if (forwardIndex instanceof MultiValueForwardIndex) {
      MultiValueForwardIndex forwardIndex2 =
          (MultiValueForwardIndex) forwardIndex;
      int[] buffer = new int[forwardIndex2.getMaxNumValuesPerDoc()];
      int count = forwardIndex2.randomRead(buffer, id);
      String[] ret = new String[count];
      while (count > 0) {
        --count;
        ret[count] = forwardIndex2.getDictionary().get(buffer[count]);
      }
      return ret;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public DocComparatorSource getDocComparatorSource() {
    return new DocComparatorSource() {
      @Override
      public DocComparator getComparator(IndexReader reader, int docbase) throws IOException {
        final ZeusDataCache zeusDataCache =
            BaFacetHandler.this.load((BoboIndexReader) reader);
        if (zeusDataCache.getForwardIndex() instanceof SortedForwardIndex) {
          return new SortedFacetUtils.SortedDocComparator();
        }
        if (zeusDataCache.getForwardIndex() instanceof SingleValueForwardIndex) {
          final SingleValueForwardIndex singleValueForwardIndex =
              (SingleValueForwardIndex) zeusDataCache.getForwardIndex();
          return new DocComparator() {
            @Override
            public Comparable value(ScoreDoc doc) {
              int index = singleValueForwardIndex.getValueIndex(doc.doc);
              return zeusDataCache.getForwardIndex().getDictionary().getComparableValue(index);
            }

            @Override
            public int compare(ScoreDoc doc1, ScoreDoc doc2) {
              return singleValueForwardIndex.getValueIndex(doc2.doc)
                  - singleValueForwardIndex.getValueIndex(doc1.doc);
            }
          };
        }
        if (zeusDataCache.getForwardIndex() instanceof MultiValueForwardIndex) {
          throw new UnsupportedOperationException("Sorts are not supported for multi value columns");
        }
        throw new UnsupportedOperationException();
      }
    };
  }

}
