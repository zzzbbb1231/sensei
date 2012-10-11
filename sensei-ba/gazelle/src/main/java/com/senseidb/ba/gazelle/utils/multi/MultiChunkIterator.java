package com.senseidb.ba.gazelle.utils.multi;

import java.util.Arrays;

import org.apache.lucene.util.OpenBitSet;

import com.senseidb.ba.gazelle.utils.CompressedIntArray;

public class MultiChunkIterator implements MultiFacetIterator {
  private final int[] skipList;
  private final int[] offsets;
  private final OpenBitSet openBitSet;
  private final CompressedIntArray compressedIntArray;
  private final int startElement;
  private final int bitSetSize;
  
  private int previousSkipListIndex = -1;
  private int previousBitSetIndex = -1;
  private int previousIndex;
  
  
  public MultiChunkIterator(int[] skipList, int[] offsets, OpenBitSet openBitSet, CompressedIntArray compressedIntArray, int startElement,
      int currentSize) {
    super();
    this.skipList = skipList;
    this.offsets = offsets;
    this.openBitSet = openBitSet;
    this.compressedIntArray = compressedIntArray;
    this.startElement = startElement;
    this.bitSetSize = currentSize;
    previousIndex = startElement - 1;
  }
  
  /* (non-Javadoc)
   * @see com.senseidb.ba.gazelle.utils.MultiFacetIterator#advance(int)
   */
  @Override
  public boolean advance(int index) {
    if (previousIndex == index) {
      return true;
    }
    /*if (index >=  startElement + bitSetSize || ) {
      return -1;
    }*/
    int skipListIndex = getSkipListIndex(index);
    if (skipListIndex > skipList.length) {
      previousSkipListIndex = Integer.MAX_VALUE;
      return false;
    }  
    if (skipListIndex != previousSkipListIndex) {
      previousSkipListIndex = skipListIndex;
      previousBitSetIndex = offsets[skipListIndex];
      previousIndex = skipList[skipListIndex];
    }
    int delta = index - previousIndex;
    int bitSetIndex = previousBitSetIndex;
   
    while (delta != 0 && bitSetIndex != -1) {
      bitSetIndex = openBitSet.nextSetBit(bitSetIndex + 1);
      if (bitSetIndex == -1) {
        return false;
      }
      delta--;
    }
    previousBitSetIndex = bitSetIndex;
    previousIndex = index;
    return true;
  }
  /* (non-Javadoc)
   * @see com.senseidb.ba.gazelle.utils.MultiFacetIterator#readValues(int[])
   */
  @Override
  public int readValues(int[] buffer) {
    int i = previousBitSetIndex;
    int next = openBitSet.nextSetBit(i + 1);
    if (next == -1) {
      next = bitSetSize;
    }
    int ret = 0;
    int tmp;
    while(i < next) {
      tmp = compressedIntArray.readInt(i);
      if (tmp != 0) {
        buffer[ret] = tmp;        
        ret++;
      }
      i++;
    }
    return ret;
  }
  private final int getSkipListIndex(int index) {
    int skipListIndex;
    if (previousSkipListIndex == -1) {
      if (skipList.length == 1 || index < skipList[1]) {
        skipListIndex = 0;       
      } else {
        skipListIndex = Arrays.binarySearch(skipList, index);
        if (skipListIndex < 0) {
          skipListIndex+=2;
          skipListIndex = skipListIndex * -1;          
        }
      }
    } else {
      if (skipList.length <= previousSkipListIndex + 1 || index < skipList[previousSkipListIndex + 1]) {
        skipListIndex = previousSkipListIndex;
      } else {
        skipListIndex = Arrays.binarySearch(skipList, previousSkipListIndex + 1, skipList.length, index);
        if (skipListIndex < 0) {
          skipListIndex+=2;
          skipListIndex = skipListIndex * -1;
        }
      }
    }
    return skipListIndex;
  }

  public int getStartElement() {
    return startElement;
  }
  
  
}
