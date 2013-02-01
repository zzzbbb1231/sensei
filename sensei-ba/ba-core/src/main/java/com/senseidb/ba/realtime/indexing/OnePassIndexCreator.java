package com.senseidb.ba.realtime.indexing;

import org.springframework.util.Assert;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.creators.MetadataCreator;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.MultiValueForwardIndexImpl1;
import com.senseidb.ba.gazelle.impl.SecondarySortedForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.SortedForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.HeapCompressedIntArray;
import com.senseidb.ba.gazelle.utils.OffHeapCompressedIntArray;
import com.senseidb.ba.gazelle.utils.multi.CompressedMultiArray;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;
import com.senseidb.ba.realtime.domain.SingleValueSearchSnapshot;
import com.senseidb.ba.realtime.domain.multi.MultiArray;

public class OnePassIndexCreator {
 
   
    
    public static ForwardIndex build(ColumnSearchSnapshot searchSnapshot,  int[] permArray, DictionarySnapshot dictionarySnapshot, TermValueList dictionary, ColumnType columnType, String columnName, int count) {
      boolean isSorted = true;
      int prevBiggerThanNextCount = 0;
      int numberOfChanges = 0;
      int dictionarySize = dictionary.size();
      boolean isSecondarySorted = false;
      IntList dictPermutationArray = dictionarySnapshot.getDictPermutationArray();
      IntList invPermutationArray = dictionarySnapshot.getInvPermutationArray();
      if (searchSnapshot instanceof SingleValueSearchSnapshot) {
        int[] forwardIndex = (int[]) searchSnapshot.getForwardIndex();
      Assert.state(forwardIndex.length == count, "Count = " + count);
      for (int i = 0; i < forwardIndex.length - 1; i++) {
        int val1 = invPermutationArray.getInt(forwardIndex[permArray[i]]);
        int val2 = invPermutationArray.getInt(forwardIndex[permArray[i + 1]]);
        if (val1 > val2) {
          isSorted = false;
          prevBiggerThanNextCount++;
        }
        if (val1 != val2) numberOfChanges++;
      }
      isSecondarySorted = (numberOfChanges * 12 + prevBiggerThanNextCount * 40) / 2 <  count / 8  * OffHeapCompressedIntArray.getNumOfBits(dictionary.size()) ;
      if (isSorted) {
       
        SortedForwardIndexImpl sortedForwardIndexImpl = new SortedForwardIndexImpl(dictionary, new int[dictionary.size()], new int[dictionary.size()], count, MetadataCreator.createMetadata(columnName, dictionary, columnType, count, true));
        sortedForwardIndexImpl.setLength(count);
        for (int i = 0; i < forwardIndex.length ; i++) {
         
            int dictionaryValueId = invPermutationArray.getInt(forwardIndex[permArray[i]]) - 1;
            if (dictionaryValueId >= dictionarySize) {
              throw new IllegalStateException();
            }
            sortedForwardIndexImpl.add(i, dictionaryValueId);
          
        }
        sortedForwardIndexImpl.seal();
        return sortedForwardIndexImpl;
      } else if (isSecondarySorted) {
        SecondarySortedForwardIndexImpl secondarySortedForwardIndexImpl = new SecondarySortedForwardIndexImpl(dictionary);
        for (int i = 0; i < forwardIndex.length ; i++) {
          int dictionaryValueId = invPermutationArray.getInt(forwardIndex[permArray[i]]) - 1;
          if (dictionaryValueId >= dictionarySize) {
            throw new IllegalStateException();
          }
          secondarySortedForwardIndexImpl.add(i, dictionaryValueId);
        }
        ColumnMetadata metadata = MetadataCreator.createSecondarySortMetadata(columnName, dictionary, columnType, count);
        secondarySortedForwardIndexImpl.seal(metadata);
        return secondarySortedForwardIndexImpl;
      } else {
        OffHeapCompressedIntArray heapCompressedIntArray = new OffHeapCompressedIntArray(count, OffHeapCompressedIntArray.getNumOfBits(dictionary.size()));
        for (int i = 0; i < forwardIndex.length ; i++) {
          int dictionaryValueId = invPermutationArray.getInt(forwardIndex[permArray[i]]) - 1;          
          if (dictionaryValueId >= dictionarySize || dictionaryValueId < 0) {
            throw new IllegalStateException();
          }
          heapCompressedIntArray.setInt(i, dictionaryValueId);
        }
        for (int i = 0; i < forwardIndex.length ; i++) {
          int dictionaryValueId = heapCompressedIntArray.getInt(i);
          if (dictionaryValueId >= dictionarySize) {
            throw new IllegalStateException();
          }        
        }
        ColumnMetadata metadata = MetadataCreator.createMetadata(columnName, dictionary, columnType, count, false);
        return new GazelleForwardIndexImpl(columnName, heapCompressedIntArray, dictionary, metadata);
      }
      } else {
        MultiArray multiArray = (MultiArray) searchSnapshot.getForwardIndex();
        Assert.state(searchSnapshot.getForwardIndexSize() == count, "Count = " + count);
        long initialSize = multiArray.getIndexArray()[multiArray.getCurrentIndex() - 1] + multiArray.getMaxNumValuesPerDoc();
        if (initialSize > Integer.MAX_VALUE) {
          initialSize = Integer.MAX_VALUE;
        }
        CompressedMultiArray compressedMultiArray = new CompressedMultiArray(OffHeapCompressedIntArray.getNumOfBits(dictionary.size()), (int) initialSize);
        int[] buffer = new int[multiArray.getMaxNumValuesPerDoc()];
        for (int i = 0; i < searchSnapshot.getForwardIndexSize(); i++) {
         
            int readCount = multiArray.readValues(buffer, permArray[i]);
          for (int j = 0; j < readCount; j++) {
            buffer[j] = invPermutationArray.getInt(buffer[j]) - 1;
          }
          compressedMultiArray.add(buffer, readCount);
        }
        compressedMultiArray.initSkipLists();
        ColumnMetadata metadata = MetadataCreator.createMultiMetadata(columnName, dictionary, columnType, count);
        return new MultiValueForwardIndexImpl1(columnName, compressedMultiArray, dictionary, metadata);
      }
      //throw new UnsupportedOperationException();
    }
   
    
    
}
