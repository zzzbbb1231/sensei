package com.senseidb.ba.facet;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.ScoreDoc;
import org.jboss.netty.util.internal.ConcurrentHashMap;

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
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.SecondarySortedForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SortedForwardIndex;
import com.senseidb.ba.gazelle.impl.SecondarySortedForwardIndexImpl;
import com.senseidb.ba.util.QueryUtils;

public class BaFacetHandler extends FacetHandler<ZeusDataCache> {
  private final String bootsrapFacetHandlerName;
  private final String columnName;
  /**
   * Needed to detect columnType collisions. Because of the schemaless approach the same column for different segments might have different types
   */
  private  Set<ColumnType> currentColumnTypes = Collections.synchronizedSet(new HashSet<ColumnType>());
 
   
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
    
    IndexSegment offlineSegment =(IndexSegment) reader.getFacetData(bootsrapFacetHandlerName);
    ForwardIndex forwardIndex = offlineSegment.getForwardIndex(columnName);
    if (forwardIndex == null) {
      return null;
    }
    currentColumnTypes.add(forwardIndex.getColumnType());  
    return new ZeusDataCache(forwardIndex, offlineSegment.getInvertedIndex(columnName));

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
        return BaFacetHandler.this.getDocIdSet(zeusDataCache, index);
      }

     
    };
  }
  public RandomAccessDocIdSet getDocIdSet(final ZeusDataCache zeusDataCache, final int index) {
    if (zeusDataCache.getForwardIndex() instanceof SortedForwardIndex) {
      return new SortedFacetUtils.SortedForwardDocIdSet((SortedForwardIndex) zeusDataCache.getForwardIndex(), index);
    }
    // Go by inverted index path
    if (zeusDataCache.invertedIndexPresent(index)) {
      final DocIdSet invertedIndex =
          zeusDataCache.getInvertedIndexes()[index];
      return new FacetUtils.InvertedIndexDocIdSet(zeusDataCache, invertedIndex, index);
    } else if (zeusDataCache.getForwardIndex() instanceof SecondarySortedForwardIndexImpl) {
      SecondarySortedForwardIndexImpl secondarySortedForwardIndexImpl = (SecondarySortedForwardIndexImpl) zeusDataCache.getForwardIndex();
      return new SortedFacetUtils.SecondarySortedForwardDocIdSet(secondarySortedForwardIndexImpl.getSortedRegions(), index);
    } else if (zeusDataCache.getForwardIndex() instanceof SingleValueForwardIndex) {
      return new FacetUtils.ForwardDocIdSet(((SingleValueForwardIndex) zeusDataCache.getForwardIndex()), index);
    } else if (zeusDataCache.getForwardIndex() instanceof MultiValueForwardIndex) {
      return new MultiFacetUtils.MultiForwardDocIdSet(((MultiValueForwardIndex) zeusDataCache.getForwardIndex()), index);
    }

    throw new UnsupportedOperationException();
  }
  private RandomAccessFilter getRangeRandomAccessFilter(final String value, final String[] values, Properties selectionProperty) {
    return new RandomAccessFilter() {
      @Override
      public double getFacetSelectivity(BoboIndexReader reader) {
        final ZeusDataCache zeusDataCache = BaFacetHandler.this.load(reader);
        return 0.3;
      }

      @Override
      public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader) throws IOException {
        final ZeusDataCache zeusDataCache = BaFacetHandler.this.load(reader);
        final int startIndex;
        final int endIndex;
        int sIndex;
        int eIndex;
        
        if (values[0].equals("*")) {
          sIndex = 0;
        } else {
          sIndex= zeusDataCache.getDictionary().indexOf(values[0]);
        }
        if (values[1].equals("*")) {
          eIndex = zeusDataCache.getDictionary().size() - 1;
        } else {
          eIndex = zeusDataCache.getDictionary().indexOf(values[1]);
        }
        
        if (sIndex < 0) {
          sIndex = -(sIndex + 1);
        } else {
          switch(QueryUtils.getStarIndexRangeType(value)) {
            case EXCLUSIVE:
              if (!values[0].equals("*")) {
                sIndex += 1;
              }
            break;
          }
        }
        
        if (eIndex < 0) {
          eIndex = -(eIndex + 1);
          eIndex = Math.max(0, eIndex - 1);
        } else {
          switch(QueryUtils.getEndIndexRangeType(value)) {
            case EXCLUSIVE:
              if (!values[1].equals("*")) {
                eIndex -= 1;
              }
            break;  
          }
        }
        
        startIndex = sIndex;
        endIndex = eIndex;
        if (startIndex > eIndex) {
          return EmptyDocIdSet.getInstance();
        }
        if (startIndex == eIndex) {
          return BaFacetHandler.this.getDocIdSet(zeusDataCache, startIndex);
        } else if (zeusDataCache.getForwardIndex() instanceof SortedForwardIndex) {
          return new SortedFacetUtils.RangeSortedForwardDocIdSet((SortedForwardIndex) zeusDataCache.getForwardIndex(), startIndex, endIndex);
        } else    if (zeusDataCache.getForwardIndex() instanceof SecondarySortedForwardIndex) {
          SecondarySortedForwardIndex secondarySortedForwardIndex = (SecondarySortedForwardIndex) zeusDataCache.getForwardIndex();
          return new SortedFacetUtils.SecondarySortedRangeForwardDocIdSet(secondarySortedForwardIndex.getSortedRegions(), startIndex, endIndex);
        } else if (zeusDataCache.getForwardIndex() instanceof SingleValueForwardIndex) {
          return new FacetUtils.RangeForwardDocIdSet(((SingleValueForwardIndex) zeusDataCache.getForwardIndex()), startIndex, endIndex);
        }  else if (zeusDataCache.getForwardIndex() instanceof MultiValueForwardIndex) {
          return new MultiFacetUtils.RangeMultiForwardDocIdSet(((MultiValueForwardIndex) zeusDataCache.getForwardIndex()), startIndex, endIndex);
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
        if (forwardIndex instanceof SecondarySortedForwardIndex) {
          SecondarySortedForwardIndex secondarySortedForwardIndex = (SecondarySortedForwardIndex) forwardIndex;
          return new SortedFacetUtils.SecondarySortFacetCountCollector(secondarySortedForwardIndex.getSortedRegions(), getName(), fakeCache, docBase, sel, fspec);
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
    ZeusDataCache dataCache = load(reader);
    if (dataCache == null) {
      return SumGroupByFacetHandler.EMPTY_STRING;
    }
    ForwardIndex forwardIndex = dataCache.getForwardIndex();
    if (forwardIndex instanceof SortedForwardIndex) {
      int dictionaryValueId =
          SortedFacetUtils.getDictionaryValueId((SortedForwardIndex) forwardIndex, id);
      if (dictionaryValueId < 0) {
        return SumGroupByFacetHandler.EMPTY_STRING;
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
          if (currentColumnTypes.size() > 1) {
            final DecimalFormat formatter =  SortedFacetUtils.formatter.get();
            return new DocComparator() {
              @Override
              public int compare(ScoreDoc doc1, ScoreDoc doc2) {
                return doc2.doc - doc1.doc;
              }

              @Override
              public Comparable value(ScoreDoc doc) {
                return formatter.format(doc.doc);
              }

            };
          }
          return new SortedFacetUtils.SortedDocComparator();
        }
        if (zeusDataCache.getForwardIndex() instanceof SingleValueForwardIndex) {
          final SingleValueForwardIndex singleValueForwardIndex =
              (SingleValueForwardIndex) zeusDataCache.getForwardIndex();
          if (currentColumnTypes.size() > 1) {
            //we should always return Strings as we have type collisions for different segments
            return new DocComparator() {
              @Override
              public Comparable value(ScoreDoc doc) {
                int index = singleValueForwardIndex.getValueIndex(doc.doc);
                return zeusDataCache.getForwardIndex().getDictionary().get(index);
              }

              @Override
              public int compare(ScoreDoc doc1, ScoreDoc doc2) {
                return singleValueForwardIndex.getValueIndex(doc2.doc)
                    - singleValueForwardIndex.getValueIndex(doc1.doc);
              }
            };
          } else 
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
