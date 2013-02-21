package com.senseidb.ba.facet;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
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
import com.senseidb.ba.file.http.FileManagementServlet;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.SecondarySortedForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.ba.gazelle.SortedForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.impl.SecondarySortedForwardIndexImpl;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.MultiValueSearchSnapshot;
import com.senseidb.ba.realtime.domain.SingleValueSearchSnapshot;
import com.senseidb.ba.realtime.facet.RealtimeFacetUtils;
import com.senseidb.ba.util.QueryUtils;

public class BaFacetHandler extends FacetHandler<ZeusDataCache> {
  private static Logger logger = Logger.getLogger(BaFacetHandler.class);  
  private final String bootsrapFacetHandlerName;
  private final String columnName;
  
  private final static int DICT_SIZE_THRESHOLD = 100000;
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
    if(offlineSegment instanceof GazelleIndexSegmentImpl && offlineSegment.getDictionary(columnName).size() >= DICT_SIZE_THRESHOLD){
    	return new ZeusDataCache(forwardIndex, ((GazelleIndexSegmentImpl) offlineSegment).getInvertedIndexObject(columnName));
    }
    else{
        return new ZeusDataCache(forwardIndex, offlineSegment.getInvertedIndex(columnName));
    }

    

  }

  private RandomAccessFilter getDefaultRandomAccessFilter(final String value, Properties selectionProperty) {
    return new RandomAccessFilter() {
      @Override
      public double getFacetSelectivity(BoboIndexReader reader) {
        
        final ZeusDataCache zeusDataCache = BaFacetHandler.this.load(reader);
        if (zeusDataCache == null) {
          return 1.0;
        }
        final int index = zeusDataCache.getDictionary().indexOf(value);
        if (index < 0)
          return 0.0;
        return ((double) zeusDataCache.getForwardIndex().getDictionary().size())
            / zeusDataCache.getForwardIndex().getLength();
      }

      @Override
      public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader) throws IOException {
        final ZeusDataCache zeusDataCache = BaFacetHandler.this.load(reader);
        if (zeusDataCache == null) {
          return EmptyDocIdSet.getInstance();
        }
        if (zeusDataCache.getForwardIndex() instanceof SingleValueSearchSnapshot) {
          return new RealtimeFacetUtils.RealtimeSingleValueDocIdSet((SingleValueSearchSnapshot)zeusDataCache.getForwardIndex(), value);
        }
        //realtime multi value can go by usual path
        final int index = zeusDataCache.getDictionary().indexOf(value);
        if (index < 0) {
          return EmptyDocIdSet.getInstance();
        }
        return BaFacetHandler.this.getDocIdSet(zeusDataCache, index);
      }

     
    };
  }
  public RandomAccessDocIdSet getDocIdSet(final ZeusDataCache zeusDataCache, final int index) {
    if (zeusDataCache == null) {
      return EmptyDocIdSet.getInstance();
    }
    if (zeusDataCache.getForwardIndex() instanceof SortedForwardIndex) {
      return new SortedFacetUtils.SortedForwardDocIdSet((SortedForwardIndex) zeusDataCache.getForwardIndex(), index);
    }
    // Go by inverted index path
    if (zeusDataCache.invertedIndexPresent(index)) {
      if(zeusDataCache.highCardinality()){
    	  final DocIdSet invertedIndex = zeusDataCache.getInvertedIndex(index);
    	  return new FacetUtils.InvertedIndexDocIdSet(zeusDataCache, invertedIndex, index);
      }
      else{
    	  final DocIdSet invertedIndex = zeusDataCache.getInvertedIndexes()[index];
    	  return new FacetUtils.InvertedIndexDocIdSet(zeusDataCache, invertedIndex, index);
      }
    } else if (zeusDataCache.getForwardIndex() instanceof SecondarySortedForwardIndexImpl) {
      SecondarySortedForwardIndexImpl secondarySortedForwardIndexImpl = (SecondarySortedForwardIndexImpl) zeusDataCache.getForwardIndex();
      return new SortedFacetUtils.SecondarySortedForwardDocIdSet(secondarySortedForwardIndexImpl.getSortedRegions(), index);
    } else if (zeusDataCache.getForwardIndex() instanceof GazelleForwardIndexImpl) {
      return new FacetUtils.ForwardDocIdSet(((GazelleForwardIndexImpl) zeusDataCache.getForwardIndex()), index);
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
        if (zeusDataCache == null) {
          return 1.0;
        }
        return 0.3;
      }

      @Override
      public RandomAccessDocIdSet getRandomAccessDocIdSet(BoboIndexReader reader) throws IOException {
        final ZeusDataCache zeusDataCache = BaFacetHandler.this.load(reader);
        if (zeusDataCache == null) {
          return EmptyDocIdSet.getInstance();
        }
        if (zeusDataCache.getForwardIndex() instanceof ColumnSearchSnapshot) {
          return handleRealtimeRangeQuery(value, values, zeusDataCache);
        }
        return handleRangeQuery(value, values, zeusDataCache);
        
      }

      public RandomAccessDocIdSet handleRangeQuery(final String value, final String[] values, final ZeusDataCache zeusDataCache) {
        final int startIndex;
        final int endIndex;
        int [] rangeIndex = QueryUtils.getRangeIndexes(zeusDataCache, value, values);
        startIndex = rangeIndex[0];
        
        if (rangeIndex[1] >= zeusDataCache.getDictionary().size()) {
          logger.warn("Got endIndex more than dictionary size for value " + value);
          endIndex = zeusDataCache.getDictionary().size() - 1;
        } else {
          endIndex = rangeIndex[1];
        }
        if (startIndex > endIndex) {
          return EmptyDocIdSet.getInstance();
        }
       
        if (startIndex == endIndex) {
          return BaFacetHandler.this.getDocIdSet(zeusDataCache, startIndex);
        } else if (zeusDataCache.getForwardIndex() instanceof GazelleForwardIndexImpl) {
          return new FacetUtils.RangeForwardDocIdSet((GazelleForwardIndexImpl) zeusDataCache.getForwardIndex(), startIndex, endIndex);
        } else if (zeusDataCache.getForwardIndex() instanceof MultiValueForwardIndex) {
          return new MultiFacetUtils.RangeMultiForwardDocIdSet(((MultiValueForwardIndex) zeusDataCache.getForwardIndex()), startIndex, endIndex);
        } else if (zeusDataCache.getForwardIndex() instanceof SortedForwardIndex) {
          return new SortedFacetUtils.RangeSortedForwardDocIdSet((SortedForwardIndex) zeusDataCache.getForwardIndex(), startIndex, endIndex);
        } else    if (zeusDataCache.getForwardIndex() instanceof SecondarySortedForwardIndex) {
          SecondarySortedForwardIndex secondarySortedForwardIndex = (SecondarySortedForwardIndex) zeusDataCache.getForwardIndex();
          return new SortedFacetUtils.SecondarySortedRangeForwardDocIdSet(secondarySortedForwardIndex.getSortedRegions(), startIndex, endIndex);
        } else {
          throw new UnsupportedOperationException();
        }
      }

      public RandomAccessDocIdSet handleRealtimeRangeQuery(final String value, final String[] values, final ZeusDataCache zeusDataCache) {
        final int startIndex;
        final int endIndex;
        int [] rangeIndex = QueryUtils.getRangeIndexes(((ColumnSearchSnapshot)zeusDataCache.getForwardIndex()).getDictionarySnapshot(), value, values);
       startIndex = rangeIndex[0];
        
        if (rangeIndex[1] >= zeusDataCache.getDictionary().size()) {
          logger.warn("Got endIndex more than dictionary size for value " + value);
          endIndex = zeusDataCache.getDictionary().size() - 1;
        } else {
          endIndex = rangeIndex[1];
        }
        if (startIndex > endIndex) {
          return EmptyDocIdSet.getInstance();
        }
        if (startIndex > endIndex) {
          return EmptyDocIdSet.getInstance();
        }
        if (zeusDataCache.getForwardIndex() instanceof SingleValueSearchSnapshot) {
          return new RealtimeFacetUtils.RealtimeRangeSingleValueDocIdSet((SingleValueSearchSnapshot)zeusDataCache.getForwardIndex(), startIndex, endIndex);
        } else if (zeusDataCache.getForwardIndex() instanceof MultiValueSearchSnapshot) {
          return new MultiFacetUtils.RangeMultiForwardDocIdSet((MultiValueSearchSnapshot)zeusDataCache.getForwardIndex(), startIndex, endIndex);
        } else {
          throw new UnsupportedOperationException();
        }
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
        if (dataCache == null) {
          return new EmptyFacetCountCollectorSource(BaFacetHandler.this._name);
        }
        final ForwardIndex forwardIndex = dataCache.getForwardIndex();

        final FacetDataCache fakeCache = dataCache.getFakeCache();
        if (forwardIndex instanceof SortedForwardIndex) {
          return new SortedFacetUtils.SortedFacetCountCollector((SortedForwardIndex) forwardIndex, getName(), fakeCache, docBase, sel, fspec);
        }
        if (forwardIndex instanceof SecondarySortedForwardIndex) {
          SecondarySortedForwardIndex secondarySortedForwardIndex = (SecondarySortedForwardIndex) forwardIndex;
          return new SortedFacetUtils.SecondarySortFacetCountCollector(secondarySortedForwardIndex.getSortedRegions(), getName(), fakeCache, docBase, sel, fspec);
        }
        if (forwardIndex instanceof GazelleForwardIndexImpl) {
          return new FacetUtils.ForwardIndexCountCollector(getName(), dataCache.getFakeCache(), (GazelleForwardIndexImpl) forwardIndex, docBase, sel, fspec);
        }
        if (forwardIndex instanceof MultiValueSearchSnapshot) {
          return new RealtimeFacetUtils.RealtimeMultiValueCountCollector(getName(), fakeCache, (MultiValueSearchSnapshot) forwardIndex, docBase, sel, fspec);
        }
        if (forwardIndex instanceof MultiValueForwardIndex) {
          return new MultiFacetUtils.MultiForwardIndexCountCollector(getName(), dataCache.getFakeCache(), (MultiValueForwardIndex) forwardIndex, docBase, sel, fspec);
        } 
        if (forwardIndex instanceof SingleValueSearchSnapshot) {
          return new RealtimeFacetUtils.RealtimeSingleValueCountCollector(getName(), fakeCache, (SingleValueSearchSnapshot) forwardIndex, docBase, sel, fspec);
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
      int dictionaryValueId = SortedFacetUtils.getDictionaryValueId((SortedForwardIndex) forwardIndex, id);
      if (dictionaryValueId < 0) {
        return SumGroupByFacetHandler.EMPTY_STRING;
      } else {
        return new String[] { forwardIndex.getDictionary().get(dictionaryValueId) };
      }
    }
    
    if (forwardIndex instanceof SingleValueForwardIndex) {
      SingleValueForwardIndex forwardIndex2 =
          (SingleValueForwardIndex) forwardIndex;
      int valueIndex = forwardIndex2.getReader().getValueIndex(id);      
      return new String[] { forwardIndex2.getDictionary().get(valueIndex) };
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
    throw new UnsupportedOperationException(forwardIndex.getClass().toString());
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
          final SingleValueRandomReader randomReader =
              ((SingleValueForwardIndex) zeusDataCache.getForwardIndex()).getReader();
          if (currentColumnTypes.size() > 1) {
            //we should always return Strings as we have type collisions for different segments
            return new DocComparator() {
              @Override
              public Comparable value(ScoreDoc doc) {
                int index = randomReader.getValueIndex(doc.doc);
                return (Comparable) zeusDataCache.getForwardIndex().getDictionary().getRawValue(index);
              }

              @Override
              public int compare(ScoreDoc doc1, ScoreDoc doc2) {
                return randomReader.getValueIndex(doc2.doc)
                    - randomReader.getValueIndex(doc1.doc);
              }
            };
          } else 
          return new DocComparator() {
            @Override
            public Comparable value(ScoreDoc doc) {
              int index = randomReader.getValueIndex(doc.doc);
              return (Comparable) zeusDataCache.getForwardIndex().getDictionary().getRawValue(index);
            }

            @Override
            public int compare(ScoreDoc doc1, ScoreDoc doc2) {
              return randomReader.getValueIndex(doc2.doc)
                  - randomReader.getValueIndex(doc1.doc);
            }
          };
          
        }
        if (zeusDataCache.getForwardIndex() instanceof MultiValueForwardIndex) {
          throw new UnsupportedOperationException("Sorts are not supported for multi value columns");
        }
        throw new UnsupportedOperationException(zeusDataCache.getForwardIndex().getClass().toString());
      }
    };
  }

}
