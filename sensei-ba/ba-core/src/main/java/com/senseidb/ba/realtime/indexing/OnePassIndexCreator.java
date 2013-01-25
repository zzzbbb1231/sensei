package com.senseidb.ba.realtime.indexing;

import it.unimi.dsi.fastutil.ints.IntList;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.creators.MetadataCreator;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.SecondarySortedForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.SortedForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.HeapCompressedIntArray;
import com.senseidb.ba.gazelle.utils.OffHeapCompressedIntArray;

public class OnePassIndexCreator {
 
   
    
    public static ForwardIndex build(int[] forwardIndex,  int[] permArray, IntList dictPermutationArray, TermValueList dictionary, ColumnType columnType, String columnName, int count) {
      boolean isSorted = true;
      int prevBiggerThanNextCount = 0;
      int numberOfChanges = 0;
      boolean isSecondarySorted = false;
      for (int i = 0; i < forwardIndex.length - 1; i++) {
        int val1 = dictPermutationArray.getInt(forwardIndex[permArray[i]]);
        int val2 = dictPermutationArray.getInt(forwardIndex[permArray[i + 1]]);
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
          sortedForwardIndexImpl.add(i, dictPermutationArray.getInt(forwardIndex[permArray[i]]) - 1);
        }
        sortedForwardIndexImpl.seal();
        return sortedForwardIndexImpl;
      } else if (isSecondarySorted) {
        SecondarySortedForwardIndexImpl secondarySortedForwardIndexImpl = new SecondarySortedForwardIndexImpl(dictionary);
        for (int i = 0; i < forwardIndex.length ; i++) {
          secondarySortedForwardIndexImpl.add(i, dictPermutationArray.getInt(forwardIndex[permArray[i]]) - 1);
        }
        ColumnMetadata metadata = MetadataCreator.createSecondarySortMetadata(columnName, dictionary, columnType, count);
        secondarySortedForwardIndexImpl.seal(metadata);
        return secondarySortedForwardIndexImpl;
      } else {
        HeapCompressedIntArray heapCompressedIntArray = new HeapCompressedIntArray(count, OffHeapCompressedIntArray.getNumOfBits(dictionary.size()));
        for (int i = 0; i < forwardIndex.length ; i++) {
          heapCompressedIntArray.setInt(i, dictPermutationArray.getInt(forwardIndex[permArray[i]]) - 1);
        }
        ColumnMetadata metadata = MetadataCreator.createMetadata(columnName, dictionary, columnType, count, false);
        return new GazelleForwardIndexImpl(columnName, heapCompressedIntArray, dictionary, metadata);
      }
      //throw new UnsupportedOperationException();
    }
   
    
    
}
