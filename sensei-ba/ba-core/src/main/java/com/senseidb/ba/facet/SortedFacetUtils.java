package com.senseidb.ba.facet;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.FacetCountCollector;
import com.browseengine.bobo.facets.FacetCountCollectorSource;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.browseengine.bobo.sort.DocComparator;
import com.senseidb.ba.gazelle.SortedForwardIndex;

public class SortedFacetUtils {
  public static class SortedForwardDocIdSet extends RandomAccessDocIdSet {
    private final int valueId;
    private final SortedForwardIndex forwardIndex;
    private int[] minDocIds;
    private int[] maxDocIds;

    public SortedForwardDocIdSet(SortedForwardIndex forwardIndex, int valueId) {
      this.forwardIndex = forwardIndex;
      minDocIds = forwardIndex.getMinDocIds();
      maxDocIds = forwardIndex.getMaxDocIds();
      this.valueId = valueId;
    }

    @Override
    public boolean get(int docId) {
      return forwardIndex.getMinDocIds()[valueId] <= docId
          && forwardIndex.getMaxDocIds()[valueId] >= docId;
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
      return (forwardIndex.getMinDocIds()[startValue] <= docId
          && forwardIndex.getMaxDocIds()[startValue] >= docId);
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
          if (doc >= endIndex) {
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
    private final SortedForwardIndex forwardIndex;
    private final FacetDataCache fakeCache;
    private int currentValueId = -1;
    private int[] minDocIds;
    private int[] maxDocIds;

    public SortedFacetCountCollector(SortedForwardIndex forwardIndex, String name, FacetDataCache fakeCache, int docBase, BrowseSelection sel, FacetSpec ospec) {
      super(name, fakeCache, docBase, sel, ospec);
      this.forwardIndex = forwardIndex;
      this.fakeCache = fakeCache;
      minDocIds = forwardIndex.getMinDocIds();
      maxDocIds = forwardIndex.getMaxDocIds();
    }

    @Override
    public void collect(int docid) {
      if (currentValueId == -1) {
        if (maxDocIds[1] >= docid) {
          currentValueId = 1;
        } else {
          int index =
              Arrays.binarySearch(maxDocIds, 1, maxDocIds.length, docid);
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
          int index =
              Arrays.binarySearch(maxDocIds, currentValueId, maxDocIds.length, docid);
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
    int index =
        Arrays.binarySearch(forwardIndex.getMaxDocIds(), 1, forwardIndex.getMaxDocIds().length, docId);
    if (index < 0) {
      index = (index + 1) * -1;
    }
    if (index < 0 || index >= forwardIndex.getMaxDocIds().length) {
      return -1;
    }
    return index;
  }
}
