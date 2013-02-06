package com.senseidb.ba.realtime.domain.multi;

import it.unimi.dsi.fastutil.ints.AbstractIntList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.springframework.util.Assert;

import com.browseengine.bobo.util.BigSegmentedArray;
import com.senseidb.ba.gazelle.utils.multi.MultiFacetIterator;

import scala.actors.threadpool.Arrays;

public class MultiArray {
    private List<int[]> chunks = new ArrayList<int[]>();
    private int capacity;
    
    private int[] indexArray;
    int[] currentChunk;
    int chunkIndex = -1;
    int currentChunkPosition;
    int currentIndex;
    private int maxNumValuesPerDoc;
    
    public MultiArray(int capacity) {
      this.capacity = capacity;
      indexArray = new int[capacity];
      
    }
    public void addNumbers(IntList values) {
      if (values.size() == 0) {
        values.add(1);
      }
      Assert.state(values.size() <= capacity);
      if (maxNumValuesPerDoc < values.size()) {
        maxNumValuesPerDoc = values.size();
      }
      if (chunkIndex == -1) {
        chunkIndex = 0;
        if (chunks.size() <= chunkIndex) {
          chunks.add(new int[capacity]);
        }
        currentChunk = chunks.get(chunkIndex);
      }
      if (currentChunkPosition + values.size() > capacity) {
        chunkIndex++;
        if (chunks.size() <= chunkIndex) {
          chunks.add(new int[capacity]);
        }
        currentChunk = chunks.get(chunkIndex);
        currentChunkPosition = 0;
      }
      indexArray[currentIndex++] = chunkIndex * capacity + currentChunkPosition;
      //setting up the marker of the new element
      currentChunk[currentChunkPosition] = Integer.MIN_VALUE | values.getInt(0);
      currentChunkPosition++;
      for (int i = 1; i < values.size(); i++) {
        currentChunk[currentChunkPosition++] = values.getInt(i);
      }
      
    }
    public void recycle() {
      currentIndex = 0;
      chunkIndex = -1;
      currentChunkPosition = 0;
      currentChunk = null;
      Arrays.fill(indexArray, 0);
      for (int[] chunk : chunks) {
        Arrays.fill(chunk, 0);
      }
    }
    public int readValues(int[] buffer, int docId) {
      int i = 0;
      int globalPosition = indexArray[docId];
      int[] currentChunk = chunks.get(globalPosition / capacity);
      int localPosition = (globalPosition - (globalPosition / capacity) * capacity)  ;      
      int val = currentChunk[localPosition];
      if (val == Integer.MIN_VALUE) {
        return 0;
      }
      val &= Integer.MAX_VALUE;
      while (val > 0 && localPosition < capacity - 1) {
        buffer[i] = val; 
        i++;
        val = currentChunk[++localPosition];
      }
      return i;
    }
    public class MultiArrayIterator implements MultiFacetIterator {
      int _chunkIndex = 0;
      int[] _currentChunk = chunks.get(0);
      int _currentChunkStart = 0;
      int _currentIndex = 0;
      int _currentPosition = 0;
      final IntList invPermutationArray;
      public MultiArrayIterator(IntList invPermutationArray) {
        this.invPermutationArray = invPermutationArray;
      }
      @Override
      public boolean advance(int index) {
        if (_currentIndex == index) {
          return true;
        }
        if (index >= indexArray.length) {
          return false;
        }
        int position = indexArray[index];
        if (position >= _currentChunkStart + capacity) {
          _chunkIndex = position /capacity;
          _currentChunk = chunks.get(_chunkIndex);          
          _currentChunkStart = (position - position % capacity) ;
        }
        _currentIndex = index;
        _currentPosition = position;
        if (_currentChunkStart > _currentPosition) {
          return false;
        }
        return true;
      }

      @Override
      public int readValues(int[] buffer) {
        int i = 0;
        int localPosition = _currentPosition - _currentChunkStart;
        int val = _currentChunk[localPosition];
        if (val == Integer.MIN_VALUE) {
          return 0;
        }
        val &= Integer.MAX_VALUE;
        while (val > 1 && localPosition < capacity - 1) {
          buffer[i] = val; 
          i++;
          val = _currentChunk[++localPosition];
        }
        return i;
      }

      @Override
      public int find(int fromIndex, int startIndex, int endIndex) {
        if (!advance(fromIndex)) {
          return -1;
        }
        int localPosition = _currentPosition - _currentChunkStart;
        int index = _currentIndex - 1;
        while(true) {
          int val = _currentChunk[localPosition];
          if (val == 0) {
           
            if (!advance(index + 1)) {
              return -1;
            }  
            localPosition = _currentPosition - _currentChunkStart;
            val = _currentChunk[localPosition];
            if (val == 0) {
              return -1;
            }
          }
          if (val < 0) {
            val &= Integer.MAX_VALUE;
            index++;
          }          
          val = invPermutationArray.getInt(val);          
          if (val >= startIndex && val <= endIndex) {
            return index;
          }
          localPosition++;
          
          if (localPosition >= capacity) {
            if (!advance(index + 1)) {
              return -1;
            }          
            localPosition = _currentPosition - _currentChunkStart;
          }
        }
      }

      @Override
      public int find(int fromIndex, int value) {
        if (!advance(fromIndex)) {
          return -1;
        }
        int localPosition = _currentPosition - _currentChunkStart;
        //the first value is negative
        int index = _currentIndex - 1;
        while(true) {
          int val = _currentChunk[localPosition];
          if (val == 0) {
            if (!advance(index + 1)) {
              return -1;
            }  
            localPosition = _currentPosition - _currentChunkStart;
            
            val = _currentChunk[localPosition];
            if (val == 0) {
              return -1;
            }
          }
          if (val < 0) {
            //change the sign
            val &= Integer.MAX_VALUE;
            index++;
          }
          if (val == value) {
            return index;
          }
          localPosition++;
          
          if (localPosition >= capacity) {
            if (!advance(index + 1)) {
              return -1;
            }          
            localPosition = _currentPosition - _currentChunkStart;
          }
        }
      }

      @Override
      public void count(BigSegmentedArray counts, int docId) {
        if (!advance(docId)) {
          return;
        }
        int localPosition = _currentPosition - _currentChunkStart;
        int val = _currentChunk[localPosition];
        if (val == Integer.MIN_VALUE) {
          return;
        }
        val &= Integer.MAX_VALUE;
        while (val > 1 && localPosition < capacity - 1) {
          counts.add(val, counts.get(val) + 1);
          val = _currentChunk[++localPosition];
        }
      }
      
    }
    public MultiArrayIterator iterator() {
      return new MultiArrayIterator( new AbstractIntList() {
      
      @Override
      public int getInt(int index) {
        return index;
      }
      
      @Override
      public int size() {      
        return Integer.MAX_VALUE;
      }
    });
    }
    
    
    public int[] getIndexArray() {
      return indexArray;
    }
    public int getCurrentIndex() {
      return currentIndex;
    }
    public int getMaxNumValuesPerDoc() {
      return maxNumValuesPerDoc;
    }
    
    public MultiFacetIterator iterator(IntList invPermutationArray) {
      return new MultiArrayIterator(invPermutationArray);
    }
    
}
