package com.senseidb.ba.facet;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;

public class FacetUtils {
  // CountCollector: static class I
  public static final class ForwardIndexCountCollector extends DefaultFacetCountCollector {
    private final SingleValueForwardIndex forwardIndex;

    public ForwardIndexCountCollector(String name, FacetDataCache dataCache, SingleValueForwardIndex forwardIndex, int docBase, BrowseSelection sel, FacetSpec ospec) {
      super(name, dataCache, docBase, sel, ospec);
      this.forwardIndex = forwardIndex;
    }

    @Override
    public void collect(int docid) {
      int valueIndex = forwardIndex.getValueIndex(docid);
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
      return ((SingleValueForwardIndex) zeusDataCache.getForwardIndex()).getValueIndex(docId) == index;
    }
  }

  // Iterator: static class III
  public static class ForwardIndexIterator extends DocIdSetIterator {
    int doc = -1;
    private final SingleValueForwardIndex forwardIndex;
    private final int index;
    private final int length;
    public ForwardIndexIterator(SingleValueForwardIndex forwardIndex, int index) {
      this.forwardIndex = forwardIndex;
      this.index = index;
      this.length = this.forwardIndex.getLength();
    }

    @Override
    public int nextDoc() throws IOException {
      int nextValueIndexed;
      while (true) {
        doc++;
        if (length <= doc)
          return NO_MORE_DOCS;
        nextValueIndexed = forwardIndex.getValueIndex(doc);
        if (nextValueIndexed == index) {
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
    private SingleValueForwardIndex forwardIndex;
    private int index;

    public ForwardDocIdSet(SingleValueForwardIndex forwardIndex, int index) {
      this.forwardIndex = forwardIndex;
      this.index = index;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new ForwardIndexIterator(forwardIndex, index);
    }

    @Override
    public boolean get(int docId) {
      return forwardIndex.getValueIndex(docId) == index;
    }
  }

  // Range FacetDocIdSet, Iterators and CountCollectors

  public static class RangeForwardDocIdSet extends RandomAccessDocIdSet {
    private SingleValueForwardIndex forwardIndex;
    private int startIndex;
    private int endIndex;

    public RangeForwardDocIdSet(SingleValueForwardIndex forwardIndex, int startIndex, int endIndex) {
      this.forwardIndex = forwardIndex;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new RangeForwardIndexIterator(forwardIndex, startIndex, endIndex);
    }

    @Override
    public boolean get(int docId) {
      int docIdfromIndex = forwardIndex.getValueIndex(docId);
      return docIdfromIndex >= startIndex
          || docIdfromIndex <= endIndex;
    }
  }

  public static class RangeForwardIndexIterator extends DocIdSetIterator {
    int doc = -1;
    private final SingleValueForwardIndex forwardIndex;
    private final int startIndex;
    private final int endIndex;
    private final int forwardIndexLength;
    
    public RangeForwardIndexIterator(final SingleValueForwardIndex forwardIndex, final int startIndex, final int endIndex) {
      this.forwardIndex = forwardIndex;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
      this.forwardIndexLength = forwardIndex.getLength();
    }

    @Override
    public int nextDoc() throws IOException {
      int nextValueIndexed;
      while (true) {
        doc++;
        if (this.forwardIndexLength <= doc) {
          return NO_MORE_DOCS;
        }
          
        nextValueIndexed = forwardIndex.getValueIndex(doc);
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
