package com.senseidb.ba.facet;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.browseengine.bobo.sort.DocComparator;
import com.senseidb.ba.gazelle.SecondarySortedForwardIndex.SortedRegion;
import com.senseidb.ba.gazelle.SortedForwardIndex;

public class SortedFacetUtils {
  public static final ThreadLocal<DecimalFormat> formatter = new ThreadLocal<DecimalFormat>() {
    protected DecimalFormat initialValue() {
      return new DecimalFormat("0000000000");
    }
  };
  public static class SortedRangeCountFinder  {
    
    private int currentValueId = -1;
    private int[] minDocIds;
    private int[] maxDocIds;

    public SortedRangeCountFinder(SortedRegion sortedRegion) {
    
      minDocIds = sortedRegion.getMinDocIds();
      maxDocIds = sortedRegion.getMaxDocIds();
    }
   
    public int find(int docid) {
      if (currentValueId == -1) {
        if (maxDocIds[0] >= docid) {
          currentValueId = 0;
        } else {
          while (++currentValueId < maxDocIds.length) {
            if (maxDocIds[currentValueId] >= docid) {
              return currentValueId;
            }
          }
          return -1;
        }
      } else if (docid > maxDocIds[currentValueId]) {
        while (++currentValueId < maxDocIds.length) {
          if (maxDocIds[currentValueId] >= docid) {
            return currentValueId;
          }
        }
        return -1;
      }
      return currentValueId;
    }
  }
  
  
  public static class SecondarySortFacetCountCollector extends DefaultFacetCountCollector {
    private int currentRegionIndex = -1;
    private SortedRangeCountFinder countFinder = null;
    private final SortedRegion[] regions;
    private SortedRegion currentRegion = null;
    public SecondarySortFacetCountCollector(SortedRegion[] regions, String name, FacetDataCache fakeCache, int docBase,
        BrowseSelection sel, FacetSpec ospec) {
      super(name, fakeCache, docBase, sel, ospec);
      this.regions = regions;
    }
   public boolean advance(int docid) {
     if (currentRegionIndex == -1) {
       if (regions[0].maxDocId >= docid) {
         currentRegionIndex = 0;
         currentRegion = regions[currentRegionIndex];
         countFinder = new SortedRangeCountFinder(regions[currentRegionIndex]);
         return true;
       } else {
         while (++currentRegionIndex < regions.length) {
           if (regions[currentRegionIndex].maxDocId >= docid) {
             currentRegion = regions[currentRegionIndex];
             countFinder = new SortedRangeCountFinder(regions[currentRegionIndex]);
             return true;
           }
         }
         return false;
       }
     } 
     if (docid <= currentRegion.maxDocId) {
       return true;
     } else {
       while (++currentRegionIndex < regions.length) {
         if (docid <= regions[currentRegionIndex].maxDocId) {
           currentRegion = regions[currentRegionIndex];
           countFinder = new SortedRangeCountFinder(regions[currentRegionIndex]);
           return true;
         }
       }
       return false;
     }
    
   }
    @Override
    public void collect(int docid) {
      if (!advance(docid)) {
        return;
      }
      int found = countFinder.find(docid);      
      _count.add(found, _count.get(found) + 1);
      return;
    }

    @Override
    public void collectAll() {
      for (int i = 0; i < regions[regions.length - 1].maxDocId; i++) {
        collect(i);
      }
    }
  }
  
  public static class SecondarySortedRangeForwardDocIdSet extends RandomAccessDocIdSet {
    private final int[] minDocIds;
    private final int[] maxDocIds;
    private final int actualLength;
    public SecondarySortedRangeForwardDocIdSet(SortedRegion[]regions, int startValueId, int endValueId) {
      
       minDocIds = new int[regions.length];
       maxDocIds = new int[regions.length];
      int actualLength = 0;
      for (SortedRegion region : regions) {
        int minDoc = Integer.MAX_VALUE;
        int maxDoc = Integer.MIN_VALUE;
        for (int index = startValueId; index <= endValueId; index++) {
          int currentMin = region.minDocIds[index];
          int currentMax = region.maxDocIds[index];
          if (currentMin < 0 || currentMax < 0) {
            continue;
          }
          if (minDoc > currentMin) {
            minDoc = currentMin;
          }
          if (maxDoc < currentMax) {
            maxDoc = currentMax;
          }
        }
        if (minDoc == Integer.MAX_VALUE ||  maxDoc == Integer.MIN_VALUE) {
          continue;
        }
        minDocIds[actualLength] = minDoc;
        maxDocIds[actualLength] = maxDoc;
        actualLength++;
      }
      this.actualLength = actualLength;
    }
    @Override
    public boolean get(int docId) {
     throw new UnsupportedOperationException();
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      
      if (actualLength == 0) {
        return EmptyDocIdSet.getInstance().iterator();
      }     
      return new DocIdSetIterator() {
        int doc = -1;
        int currentRegionId = -1;
        int currentMaxDoc = -1;
        @Override
        public int nextDoc() throws IOException {
          if (doc == -1) {
            currentRegionId = 0;
            doc = minDocIds[currentRegionId];
            currentMaxDoc = maxDocIds[currentRegionId];
          } else {
            doc++;
          }
          if (doc > currentMaxDoc) {
            if (doc <= maxDocIds[currentRegionId + 1]) {
              currentRegionId += 1;
              currentMaxDoc = maxDocIds[currentRegionId];
              if (doc < minDocIds[currentRegionId]) {
                doc = minDocIds[currentRegionId];
              }
              return doc;
            } else {
              int index = Arrays.binarySearch(maxDocIds, currentRegionId + 2, maxDocIds.length, doc);
              if (index < 0) {
                index = (index + 1) * -1;
              }
              currentRegionId = index;
              if (currentRegionId >= actualLength) {
                return NO_MORE_DOCS;
              }
              currentMaxDoc = maxDocIds[currentRegionId];
              if (doc < minDocIds[currentRegionId]) {
                doc = minDocIds[currentRegionId];
              }
              return doc;
            }           
          }
          return doc;          
        }
        
        @Override
        public int docID() {
          return doc;
        }
        
        @Override
        public int advance(int target) throws IOException {
          doc = target -1;
          return nextDoc();
        }
      };
    }
  }
  
  public static class SecondarySortedForwardDocIdSet extends RandomAccessDocIdSet {
    private final SortedRegion[] regions;
    private final int valueId;

    public SecondarySortedForwardDocIdSet(SortedRegion[]regions, int valueId) {
      this.regions = regions;
      this.valueId = valueId;
      
    }
    @Override
    public boolean get(int docId) {
     throw new UnsupportedOperationException();
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      int region = -1;
      for (int i = 0; i < regions.length; i++) {
        if (regions[i].getMinDocIds()[valueId] >= 0) {
          region = i;
          break;
        }
      }
      if (region == -1) {
        return EmptyDocIdSet.getInstance().iterator();
      }
      final int minRegion = region;
      return new DocIdSetIterator() {
        int doc = -1;
        int currentRegionId = minRegion;
        SortedRegion currentRegion = regions[minRegion];
        @Override
        public int nextDoc() throws IOException {
          if (doc == -1) {
            doc = currentRegion.minDocIds[valueId];          
          } else {
            doc++;
          }
          if (doc > currentRegion.maxDocIds[valueId]) {
            while (++currentRegionId < regions.length) {
              if (regions[currentRegionId].minDocIds[valueId] >= 0) {
                currentRegion = regions[currentRegionId];
                doc = currentRegion.minDocIds[valueId];
                return doc;
              }
            }
            return NO_MORE_DOCS;
          }
          return doc;          
        }
        
        @Override
        public int docID() {
          
          return doc;
        }
        
        @Override
        public int advance(int target) throws IOException {
          doc = target -1;
          return nextDoc();
        }
      };
    }
  }
  
  public static class SortedForwardDocIdSet extends RandomAccessDocIdSet {
    private final int valueId;
    private int[] minDocIds;
    private int[] maxDocIds;

    public SortedForwardDocIdSet(SortedForwardIndex forwardIndex, int valueId) {
      minDocIds = forwardIndex.getMinDocIds();
      maxDocIds = forwardIndex.getMaxDocIds();
      this.valueId = valueId;
    }
    public SortedForwardDocIdSet(int[] minDocIds, int[] maxDocIds, int valueId) {
      this.minDocIds = minDocIds;
      this.maxDocIds = maxDocIds;
      this.valueId = valueId;
    }

    @Override
    public boolean get(int docId) {
      return minDocIds[valueId] <= docId && maxDocIds[valueId] >= docId;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new DocIdSetIterator() {
        int doc = -1;

        @Override
        public int nextDoc() throws IOException {
          if (doc == -1) {
            doc = minDocIds[valueId];
          } else {
            doc++;
          }
          if (doc > maxDocIds[valueId]) {
            return NO_MORE_DOCS;
          }
          return doc;
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public int advance(int target) throws IOException {
          doc = target - 1;
          return nextDoc();
        }
      };
    }
  }

  public static class RangeSortedForwardDocIdSet extends RandomAccessDocIdSet {
    private final int startValue;
    private final int endValue;

    private final SortedForwardIndex forwardIndex;
    private int[] minDocIds;
    private int[] maxDocIds;
    private int startIndex;
    private int endIndex;

    public RangeSortedForwardDocIdSet(SortedForwardIndex forwardIndex, int startValue, int endValue) {
      this.forwardIndex = forwardIndex;
      minDocIds = forwardIndex.getMinDocIds();
      maxDocIds = forwardIndex.getMaxDocIds();
      this.startValue = startValue;
      this.endValue = endValue;
      this.startIndex = minDocIds[startValue];
      this.endIndex = maxDocIds[endValue];
    }

    @Override
    public boolean get(int docId) {
      int nextDocId = forwardIndex.getMinDocIds()[startValue];
      return (forwardIndex.getMinDocIds()[startValue] <= docId && forwardIndex.getMaxDocIds()[endValue] >= docId);
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new DocIdSetIterator() {
        int doc = -1;

        @Override
        public int nextDoc() throws IOException {
          if (doc == -1) {
            doc = startIndex;
          } else {
            doc++;
          }
          if (doc > endIndex) {
            return NO_MORE_DOCS;
          }
          return doc;
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public int advance(int target) throws IOException {
          doc = target - 1;
          return nextDoc();
        }
      };
    }
  }

  @SuppressWarnings("rawtypes")
  public static class SortedFacetCountCollector extends DefaultFacetCountCollector {
    private final FacetDataCache fakeCache;
    private int currentValueId = -1;
    private int[] minDocIds;
    private int[] maxDocIds;

    public SortedFacetCountCollector(SortedForwardIndex forwardIndex, String name, FacetDataCache fakeCache, int docBase,
        BrowseSelection sel, FacetSpec ospec) {
      super(name, fakeCache, docBase, sel, ospec);
      this.fakeCache = fakeCache;
      minDocIds = forwardIndex.getMinDocIds();
      maxDocIds = forwardIndex.getMaxDocIds();
    }
    public SortedFacetCountCollector(int[] minDocIds, int[] maxDocIds, String name, FacetDataCache fakeCache, int docBase,
        BrowseSelection sel, FacetSpec ospec) {
      super(name, fakeCache, docBase, sel, ospec);
      this.fakeCache = fakeCache;
      this.minDocIds = minDocIds;
      this.maxDocIds = maxDocIds;
    }
    @Override
    public void collect(int docid) {
      if (currentValueId == -1) {
        if (maxDocIds[1] >= docid) {
          currentValueId = 1;
        } else {
          int index = Arrays.binarySearch(maxDocIds, 1, maxDocIds.length, docid);
          if (index < 0) {
            index = (index + 1) * -1;
          }
          currentValueId = index;
          if (index >= maxDocIds.length) {
            return;
          }
        }
      } else if (docid > maxDocIds[currentValueId]) {
        currentValueId++;
        if (currentValueId == maxDocIds.length) {
          return;
        }
        if (docid > maxDocIds[currentValueId]) {
          int index = Arrays.binarySearch(maxDocIds, currentValueId, maxDocIds.length, docid);
          if (index < 0) {
            index = (index + 1) * -1;
          }
          currentValueId = index;
          if (index >= maxDocIds.length) {
            return;
          }
        }

      }
      _count.add(currentValueId, _count.get(currentValueId) + 1);
      return;

    }

    @Override
    public void collectAll() {
      for (int i = 0; i < maxDocIds.length; i++) {
        _count.add(i, maxDocIds[i] - minDocIds[i] + 1);
      }

    }

  }
 
  
  
  
  
  public static class SortedDocComparator extends DocComparator {
    @Override
    public int compare(ScoreDoc doc1, ScoreDoc doc2) {
      return doc2.doc - doc1.doc;
    }

    @Override
    public Comparable value(ScoreDoc doc) {
      return doc.doc;
    }

  }

  public static int getDictionaryValueId(SortedForwardIndex forwardIndex, int docId) {
    int index = Arrays.binarySearch(forwardIndex.getMaxDocIds(), 1, forwardIndex.getMaxDocIds().length, docId);
    if (index < 0) {
      index = (index + 1) * -1;
    }
    if (index < 0 || index >= forwardIndex.getMaxDocIds().length) {
      return -1;
    }
    return index;
  }
}
