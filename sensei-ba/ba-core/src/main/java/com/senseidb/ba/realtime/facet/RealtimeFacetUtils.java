package com.senseidb.ba.realtime.facet;

import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.SingleValueSearchSnapshot;
import com.senseidb.ba.util.QueryUtils;

public class RealtimeFacetUtils {
  public static class RealtimeSingleValueDocIdSet extends RandomAccessDocIdSet {
    private ColumnSearchSnapshot forwardIndex;
    private int index;

    public RealtimeSingleValueDocIdSet(SingleValueSearchSnapshot forwardIndex, String value) {
      this.forwardIndex = forwardIndex;
      this.index = forwardIndex.getDictionarySnapshot().sortedIndexOf(value);
    }
    public RealtimeSingleValueDocIdSet(SingleValueSearchSnapshot forwardIndex, int index) {
      this.forwardIndex = forwardIndex;
      this.index = index;
    }
    @Override
    public DocIdSetIterator iterator() throws IOException {
      if (index < 0) {
        return EmptyDocIdSet.getInstance().iterator();
      }
      final int unsortedValue = forwardIndex.getDictionarySnapshot().getDictPermutationArray().getInt(index);
      return new SingleValueForwardIndexIterator((int[])forwardIndex.getForwardIndex(), forwardIndex.getForwardIndexSize(), unsortedValue);
    }

    @Override
    public boolean get(int docId) {
      throw new UnsupportedOperationException();
    }
  }
  public static class RealtimeRangeSingleValueDocIdSet extends RandomAccessDocIdSet {
    private SingleValueSearchSnapshot forwardIndex;
    private String value;
    private final int startIndex;
    private final int endIndex;

    public RealtimeRangeSingleValueDocIdSet(SingleValueSearchSnapshot forwardIndex, int startIndex, int endIndex) {
      this.startIndex = startIndex;
      this.endIndex = endIndex;
     
      
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      if (startIndex > endIndex) {
        return EmptyDocIdSet.getInstance().iterator();
      }
      if (startIndex == endIndex) {
        return new RealtimeSingleValueDocIdSet(forwardIndex, startIndex).iterator();
      }
     return new RealtimeRangeForwardIndexIterator((int[])forwardIndex.getForwardIndex(), forwardIndex.getForwardIndexSize(), forwardIndex.getDictionarySnapshot().getDictPermutationArray(), startIndex, endIndex);
    }

    @Override
    public boolean get(int docId) {
      throw new UnsupportedOperationException();
    }
  }
  
  public static class RealtimeRangeForwardIndexIterator extends DocIdSetIterator {
    int doc = -1;
    private int[] forwardIndex;
    private int startIndex;
    private int endIndex;
    private int forwardIndexSize;
    private IntList dictPermutationArray;
    
    public RealtimeRangeForwardIndexIterator(final int[] forwardIndex, int forwardIndexSize, IntList dictPermutationArray, final int startIndex, final int endIndex) {
      this.forwardIndex = forwardIndex;
      this.forwardIndexSize = forwardIndexSize;
      this.dictPermutationArray = dictPermutationArray;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }

    @Override
    public int nextDoc() throws IOException {
      int nextValueIndexed;
      while (true) {
        doc++;
        if (this.forwardIndexSize <= doc) {
          return NO_MORE_DOCS;
        }
          
        nextValueIndexed = forwardIndex[doc];
        if (nextValueIndexed == 0) {
          return NO_MORE_DOCS;
        }
        if (dictPermutationArray.get(nextValueIndexed) >= startIndex && dictPermutationArray.get(nextValueIndexed) <= endIndex) {
          return doc;
        }
      }
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
   
  public static class SingleValueForwardIndexIterator extends DocIdSetIterator {
    int doc = -1;   
    private final int index;
    private final int length;
    private final int[] forwardIndex;
    

    public SingleValueForwardIndexIterator(int[] forwardIndex, int forwardIndexSize, int index) {
      this.forwardIndex = forwardIndex;
      length = forwardIndexSize;
      this.index = index;
    }
    @Override
    public int nextDoc() throws IOException {
      while (true) {
        doc++;
        if (length <= doc)
          return NO_MORE_DOCS;
        if (forwardIndex[doc] == index) {
          return doc;
        }
      }
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
  public static final class RealtimeSingleValueCountCollector extends RealtimeFacetCountCollector {
    private final int[] forwardIndex;
    private  final int forwardIndexSize;

    public RealtimeSingleValueCountCollector(String name, FacetDataCache dataCache, SingleValueSearchSnapshot forwardIndex, int docBase, BrowseSelection sel, FacetSpec ospec) {
      super(name, dataCache, docBase, sel, ospec);
      this.forwardIndex = (int[])forwardIndex.getForwardIndex();
       forwardIndexSize = forwardIndex.getForwardIndexSize();
    }

    @Override
    public void collect(int docid) {
     
      int valueId = forwardIndex[docid];
      _count.add(valueId, _count.get(valueId) + 1);

    }

    @Override
    public void collectAll() {
      for (int i = 0; i < forwardIndexSize; i++) {
        collect(i);
      }

    }
  }
}
