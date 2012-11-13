package com.senseidb.ba.facet;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.utils.multi.MultiFacetIterator;

public class MultiFacetUtils {
  public static final class MultiForwardIndexCountCollector extends DefaultFacetCountCollector {
    private final MultiFacetIterator iterator;
    private final int[] buffer;
    private final MultiValueForwardIndex forwardIndex;

    @SuppressWarnings("rawtypes")
    public MultiForwardIndexCountCollector(String name, FacetDataCache dataCache, MultiValueForwardIndex forwardIndex, int docBase, BrowseSelection sel, FacetSpec ospec) {
      super(name, dataCache, docBase, sel, ospec);
      iterator = forwardIndex.getIterator();
      buffer = new int[forwardIndex.getMaxNumValuesPerDoc()];
      this.forwardIndex = forwardIndex;
    }

    @Override
    public void collect(int docid) {
      iterator.count(_count, docid);
    }

    @Override
    public void collectAll() {
      for (int i = 0; i < forwardIndex.getLength(); i++) {
        collect(i);
      }

    }
  }

  public static class MultiForwardIndexIterator extends DocIdSetIterator {
    int doc = -1;
    private final MultiValueForwardIndex forwardIndex;
    private final int index;
    private int length;
    private MultiFacetIterator iterator;

    public MultiForwardIndexIterator(MultiValueForwardIndex forwardIndex, int index) {
      this.forwardIndex = forwardIndex;
      this.index = index;
      length = forwardIndex.getLength();
      iterator = forwardIndex.getIterator();
    }

    @Override
    public int nextDoc() throws IOException {
      doc = iterator.find(doc + 1, index);
      if (doc < 0) {
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
  }

  public static class MultiForwardDocIdSet extends RandomAccessDocIdSet {
    private MultiValueForwardIndex forwardIndex;
    private int index;
    private int[] buffer;

    public MultiForwardDocIdSet(MultiValueForwardIndex forwardIndex, int index) {
      this.forwardIndex = forwardIndex;
      this.index = index;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new MultiForwardIndexIterator(forwardIndex, index);
    }

    @Override
    public boolean get(int docId) {
      buffer = new int[forwardIndex.getMaxNumValuesPerDoc()];
      int count = forwardIndex.randomRead(buffer, docId);
      while (count > 0) {
        if (index == buffer[--count]) {
          return true;
        }
      }
      return false;
    }
  }
  
  public static class RangeMultiForwardIndexIterator extends DocIdSetIterator {
    int doc = -1;
    private final MultiValueForwardIndex forwardIndex;
    private final int startIndex;
    private final int endIndex;
    private int length;
    private MultiFacetIterator iterator;

    public RangeMultiForwardIndexIterator(MultiValueForwardIndex forwardIndex, int startIndex, int endIndex) {
      this.forwardIndex = forwardIndex;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
      length = forwardIndex.getLength();
      iterator = forwardIndex.getIterator();
    }

    @Override
    public int nextDoc() throws IOException {
      doc = iterator.find((doc + 1), startIndex, endIndex);
      if (doc < 0) {
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
  }
  
  public static class RangeMultiForwardDocIdSet extends RandomAccessDocIdSet {
    private MultiValueForwardIndex forwardIndex;
    private int startindex;
    private int endIndex;
    private int[] buffer;

    public RangeMultiForwardDocIdSet(MultiValueForwardIndex forwardIndex, int startIndex, int endIndex) {
      this.forwardIndex = forwardIndex;
      this.startindex = startIndex;
      this.endIndex = endIndex;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new RangeMultiForwardIndexIterator(forwardIndex, startindex, endIndex);
    }

    @Override
    public boolean get(int docId) {
      buffer = new int[forwardIndex.getMaxNumValuesPerDoc()];
      int count = forwardIndex.randomRead(buffer, docId);
      while (count > 0) {
        if (buffer[--count] >= startindex || buffer[--count] <= endIndex) {
          return true;
        }
      }
      return false;
    }
  }
}
