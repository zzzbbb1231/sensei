package com.senseidb.ba.facet;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.docidset.EmptyDocIdSet;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.IntArray;

public class FacetUtils {
  // CountCollector: static class I
  public static final class ForwardIndexCountCollector extends DefaultFacetCountCollector {
    private final SingleValueForwardIndex forwardIndex;
    private IntArray compressedIntArray;

    public ForwardIndexCountCollector(String name, FacetDataCache dataCache, GazelleForwardIndexImpl forwardIndex, int docBase, BrowseSelection sel, FacetSpec ospec) {
      super(name, dataCache, docBase, sel, ospec);
      this.forwardIndex = forwardIndex;
      compressedIntArray = forwardIndex.getCompressedIntArray();
    }

    @Override
    public void collect(int docid) {
      int valueIndex = compressedIntArray.getInt(docid);
      _count.add(valueIndex, _count.get(valueIndex) + 1);

    }

    @Override
    public void collectAll() {
      for (int i = 0; i < forwardIndex.getLength(); i++) {
        collect(i);
      }

    }
  }

  // DocIdSet: static class II
  public static final class InvertedIndexDocIdSet extends RandomAccessDocIdSet {
    private final ZeusDataCache zeusDataCache;
    private final DocIdSet invertedIndex;
    private final int index;

    public InvertedIndexDocIdSet(ZeusDataCache zeusDataCache, DocIdSet invertedIndex, int index) {
      this.zeusDataCache = zeusDataCache;
      this.invertedIndex = invertedIndex;
      this.index = index;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return invertedIndex.iterator();
    }

    @Override
    public boolean get(int docId) {
     throw new UnsupportedOperationException();
    }
  }

  // Iterator: static class III
  public static class ForwardIndexIterator extends DocIdSetIterator {
    int doc = -1;   
    private final int index;
    private final int length;
    private final IntArray compressedIntArray;
    

    public ForwardIndexIterator(IntArray compressedIntArray, int length, int index) {
      this.compressedIntArray = compressedIntArray;
      this.length = length;
      this.index = index;
    }

    @Override
    public int nextDoc() throws IOException {
      while (true) {
        doc++;
        if (length <= doc)
          return NO_MORE_DOCS;
        if (compressedIntArray.getInt(doc) == index) {
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

  // DocIdSet: static class IV
  public static class ForwardDocIdSet extends RandomAccessDocIdSet {
    private GazelleForwardIndexImpl forwardIndex;
    private int index;

    public ForwardDocIdSet(GazelleForwardIndexImpl forwardIndex, int index) {
      this.forwardIndex = forwardIndex;
      this.index = index;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new ForwardIndexIterator(forwardIndex.getCompressedIntArray(), forwardIndex.getLength(), index);
    }

    @Override
    public boolean get(int docId) {
      return forwardIndex.getCompressedIntArray().getInt(docId) == index;
    }
  }

  // Range FacetDocIdSet, Iterators and CountCollectors

  public static class RangeForwardDocIdSet extends RandomAccessDocIdSet {
    private SingleValueForwardIndex forwardIndex;
    private int startIndex;
    private int endIndex;
    private IntArray compressedIntArray;

    public RangeForwardDocIdSet(GazelleForwardIndexImpl forwardIndex, int startIndex, int endIndex) {
      this.forwardIndex = forwardIndex;
      compressedIntArray = forwardIndex.getCompressedIntArray();
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      if (endIndex < 0) {
        return   EmptyDocIdSet.getInstance().iterator();
      }
      return new RangeForwardIndexIterator(compressedIntArray, forwardIndex.getLength(), startIndex, endIndex);
    }

    @Override
    public boolean get(int docId) {
      int docIdfromIndex = compressedIntArray.getInt(docId);
      return docIdfromIndex >= startIndex
          || docIdfromIndex <= endIndex;
    }
  }

  public static class RangeForwardIndexIterator extends DocIdSetIterator {
    int doc = -1;
    private final IntArray compressedIntArray;
    private final int startIndex;
    private final int endIndex;
    private final int forwardIndexLength;
    
    public RangeForwardIndexIterator(final IntArray compressedIntArray, int length, final int startIndex, final int endIndex) {
      this.compressedIntArray = compressedIntArray;
      forwardIndexLength = length;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }

    @Override
    public int nextDoc() throws IOException {
      int nextValueIndexed;
      while (true) {
        doc++;
        if (this.forwardIndexLength <= doc) {
          return NO_MORE_DOCS;
        }
          
        nextValueIndexed = compressedIntArray.getInt(doc);
        if (nextValueIndexed >= startIndex && nextValueIndexed <= endIndex) {
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

}
