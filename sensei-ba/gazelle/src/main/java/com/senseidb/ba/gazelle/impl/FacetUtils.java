package com.senseidb.ba.gazelle.impl;

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
 
  // Iterator: static class III
  public static class ForwardIndexIterator extends DocIdSetIterator {
    int doc = -1;   
    private final int index;
    private final IntArray compressedIntArray;
    private int finalDoc;
    

    public ForwardIndexIterator(IntArray compressedIntArray, int index, int finalDoc) {
      this.compressedIntArray = compressedIntArray;
      this.index = index;
      this.finalDoc = finalDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      while (true) {
        doc++;
        if (doc > finalDoc)
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
    private int finalDoc;

    public ForwardDocIdSet(GazelleForwardIndexImpl forwardIndex, int index, int finalDoc) {
      this.forwardIndex = forwardIndex;
      this.index = index;
      this.finalDoc = finalDoc;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new ForwardIndexIterator(forwardIndex.getCompressedIntArray(), index, finalDoc);
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
