package com.senseidb.ba.facet;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.docidset.RandomAccessDocIdSet;
import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.facets.impl.DefaultFacetCountCollector;
import com.senseidb.ba.SingleValueForwardIndex;

public class FacetUtils {
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
          return ((SingleValueForwardIndex)zeusDataCache.getForwardIndex()).getValueIndex(docId) == index;
        }
    }
    public static class ForwardIndexIterator extends DocIdSetIterator {
        int doc = -1;
        private final SingleValueForwardIndex forwardIndex;
        private final int index;
        public ForwardIndexIterator(SingleValueForwardIndex forwardIndex, int index) {
          this.forwardIndex = forwardIndex;
          this.index = index;
        }
        @Override
        public int nextDoc() throws IOException {
          while (true) {
            doc++;
            if (forwardIndex.getLength() <= doc) return NO_MORE_DOCS;
              if (forwardIndex.getValueIndex(doc) == index) {
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
}
