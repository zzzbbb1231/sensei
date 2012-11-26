package com.senseidb.ba;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import com.browseengine.bobo.facets.data.FacetDataCache;
import com.browseengine.bobo.util.BigIntArray;
import com.browseengine.bobo.util.BigSegmentedArray;
import com.kamikaze.docidset.impl.AndDocIdSet;
import com.senseidb.ba.gazelle.utils.OffHeapCompressedIntArray;

public class PerfTest {
    public static class ForwardIndexIterator extends DocIdSetIterator {
        int doc = -1;
        private final OffHeapCompressedIntArray forwardIndex;
        private final int index;
        private final int length;
        public ForwardIndexIterator(OffHeapCompressedIntArray forwardIndex, int index, int length) {
          this.forwardIndex = forwardIndex;
          this.index = index;
        this.length = length;
        }
        @Override
        public int nextDoc() throws IOException {
          while (true) {
            doc++;
            if (length <= doc) return NO_MORE_DOCS;
            if (forwardIndex.getInt(doc) == index) {
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
    
    public static class FacetDocIdSetIterator extends DocIdSetIterator
    {
        protected int _doc;
        protected final int _index;
        protected final int _maxID;
        protected final BigSegmentedArray _orderArray;
        
        public FacetDocIdSetIterator(BigSegmentedArray bigSegmentedArray,int index, int length)
        {
            _index=index;
            _doc=-1;
            _maxID = length;
            _orderArray = bigSegmentedArray;
        }
        
        @Override
        final public int docID() {
            return _doc;
        }

        @Override
        public int nextDoc() throws IOException
        {
          return (_doc = (_doc < _maxID ? _orderArray.findValue(_index, (_doc + 1), _maxID) : NO_MORE_DOCS));
        }

        @Override
        public int advance(int id) throws IOException
        {
          if (_doc < id)
          {
            return (_doc = (id <= _maxID ? _orderArray.findValue(_index, id, _maxID) : NO_MORE_DOCS));
          }
          return nextDoc();
        }
    }
    public static final class CountCollector {
        int  count = 0;
        public void collect(int doc) {
            if (doc % 2 == 0) {
                count +=1;
            }
        }
        public int getCount() {
            return count;
        }
    }
    public static class GenericIterator  {
        private AndDocIdSet andDocIdSet;
        private final CountCollector collector;
        private DocIdSetIterator iterator;

        public GenericIterator(CountCollector collector, final DocIdSetIterator...docIdSetIterators ) throws IOException {
            this.collector = collector;
            ArrayList<DocIdSet> arrayList = new ArrayList<DocIdSet>();
            for ( int i = 0; i < docIdSetIterators.length; i++) {
                final int k = i;
                
                arrayList.add(new DocIdSet() {
                    
                    @Override
                    public DocIdSetIterator iterator() throws IOException {
                        return docIdSetIterators[k];
                    }
                });
            }
             andDocIdSet = new AndDocIdSet(arrayList);
             iterator = andDocIdSet.iterator();
        }

       
public int iterate() throws IOException {
    int doc = 0;
    while((doc = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        collector.collect(doc);
    }
    return collector.getCount();
}
    }

    public static void main1(String[] args) throws IOException {
        int numDocs = 100000000;
        int dictSize = 2000;
        int step = 10;
        BigSegmentedArray _orderArray1 = new BigIntArray(numDocs);
        BigSegmentedArray _orderArray2 = new BigIntArray(numDocs);
        for (int i = 0; i < numDocs; i++) {
            _orderArray1.add(i, dictSize/2);
            if (i % step == 0)
            _orderArray2.add(i, dictSize/2);
        }
        
        while (true) {
            int count = 0;
            GenericIterator genericIterator = new GenericIterator(new CountCollector(), new FacetDocIdSetIterator(_orderArray1, dictSize/2, numDocs),
                    new FacetDocIdSetIterator(_orderArray2, dictSize/2, numDocs));
            long time = System.currentTimeMillis();
            count = genericIterator.iterate();
            time = System.currentTimeMillis() - time;
            System.out.println(time + ", count = " + count);
            
        }
    }
    public static void main(String[] args) throws IOException {
        int numDocs = 100000000;
        int dictSize = 2000;
        int step = 10;
        OffHeapCompressedIntArray _orderArray1 = new OffHeapCompressedIntArray(numDocs, OffHeapCompressedIntArray.getNumOfBits(dictSize));
        OffHeapCompressedIntArray _orderArray2 = new OffHeapCompressedIntArray(numDocs, OffHeapCompressedIntArray.getNumOfBits(dictSize));
        for (int i = 0; i < numDocs; i++) {
            _orderArray1.setInt(i, dictSize/2);
            if (i % step == 0)
            _orderArray2.setInt(i, dictSize/2);
        }
        
        while (true) {
            int count = 0;
            GenericIterator genericIterator = new GenericIterator(new CountCollector(), new ForwardIndexIterator(_orderArray1, dictSize/2, numDocs),
                    new ForwardIndexIterator(_orderArray2, dictSize/2, numDocs));
            long time = System.currentTimeMillis();
            count = genericIterator.iterate();
            time = System.currentTimeMillis() - time;
            System.out.println(time + ", count = " + count);
            
        }
    }
}
