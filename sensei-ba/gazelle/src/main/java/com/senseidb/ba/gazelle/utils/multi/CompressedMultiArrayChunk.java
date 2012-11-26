package com.senseidb.ba.gazelle.utils.multi;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.util.OpenBitSet;
import org.springframework.util.Assert;

import com.senseidb.ba.gazelle.persist.DictionaryPersistentManager;
import com.senseidb.ba.gazelle.utils.Bits;
import com.senseidb.ba.gazelle.utils.OffHeapCompressedIntArray;
import com.senseidb.ba.gazelle.utils.HeapCompressedIntArray;
import com.senseidb.ba.gazelle.utils.IntArray;
import com.senseidb.ba.gazelle.utils.ReadMode;

public class CompressedMultiArrayChunk {
  public static Logger logger =  Logger.getLogger(CompressedMultiArrayChunk.class);
  private OpenBitSet openBitSet;
  private IntArray compressedIntArray;
  private int startElement;
  private int currentSize = 0;
  private final int numBitsPerElement;
  private int[] skipList;
  private int[] offsets;
private int maxNumValuesPerDoc;
  private static final int[] ARRAY_WITH_SINGLE_ZERO = new int[] {0};
  public CompressedMultiArrayChunk(int numBitsPerElement) {
    this.numBitsPerElement = numBitsPerElement;
  }

  public CompressedMultiArrayChunk(int startElement, int numBitsPerElement, int initialSize) {
    this.startElement = startElement;
    this.numBitsPerElement = numBitsPerElement;
    compressedIntArray = new OffHeapCompressedIntArray(initialSize, numBitsPerElement);
    openBitSet = new OpenBitSet(initialSize);
  }
  public void add(int[] values, int length) {
    if (values.length == 0) {
      values = ARRAY_WITH_SINGLE_ZERO;
      length = 1;
    }
    Assert.state(skipList == null);
    ensureCapacity(currentSize + length);
    openBitSet.set(currentSize);
    for (int i = 0; i < length; i++) {
      compressedIntArray.setInt(currentSize + i, values[i]);
    }
    currentSize += length;
    
  }
  public void add(int... values) {
   add(values, values.length);
  }

  private void ensureCapacity(int newCapacity) {
    if (compressedIntArray.size() < newCapacity) {
      long capacityToSet = Math.max((long) compressedIntArray.size() * 2, newCapacity);
      Assert.state(OffHeapCompressedIntArray.getRequiredBufferSize(capacityToSet, numBitsPerElement) <= Integer.MAX_VALUE);
      if (compressedIntArray instanceof OffHeapCompressedIntArray) {
        OffHeapCompressedIntArray newCompressedArray = new OffHeapCompressedIntArray((int) capacityToSet, ((OffHeapCompressedIntArray) compressedIntArray).getNumOfBitsPerElement());
        ByteBuffer storage = ((OffHeapCompressedIntArray) compressedIntArray).getStorage();
        storage.rewind();
        storage.rewind();
        newCompressedArray.getStorage().put(storage);
        compressedIntArray = newCompressedArray;
      } else {
        HeapCompressedIntArray heapCompressedIntArray = (HeapCompressedIntArray) compressedIntArray;
        HeapCompressedIntArray newCompressedArray = new HeapCompressedIntArray((int) capacityToSet, heapCompressedIntArray.getBitsPerValue());
        for (int i = 0; i < heapCompressedIntArray.getBlocks().length; i++) {
          newCompressedArray.getBlocks()[i] = heapCompressedIntArray.getBlocks()[i];
          compressedIntArray = newCompressedArray;
        }
      }
      openBitSet.ensureCapacity(capacityToSet);
    }
  }

  public int getStartElement() {
    return startElement;
  }

  public int getCurrentSize() {
    return currentSize;
  }

  public void initSkipLists() {
    int numberOfElementsInBlock = 2048;
    IntArrayList currentOffsets = new IntArrayList(currentSize / numberOfElementsInBlock + 10);
    IntArrayList currentSkipList = new IntArrayList(currentSize / numberOfElementsInBlock + 10);
    int nextIndex = -1;
    int counter = 0;
    int i = 0;
    currentOffsets.add(0);
    currentSkipList.add(startElement);
    while ((nextIndex = openBitSet.nextSetBit(nextIndex + 1)) >= 0) {
      if (nextIndex - counter > numberOfElementsInBlock) {
        currentSkipList.add(startElement + i);
        currentOffsets.add(nextIndex);
        counter = nextIndex;
      }
      i++;
    }
    currentSkipList.trim();
    currentOffsets.trim();
    skipList = currentSkipList.elements();
    offsets = currentOffsets.elements();
    initMaxNumValuesPerDoc(openBitSet);
  }

  private void initMaxNumValuesPerDoc(OpenBitSet openBitSet) {
      maxNumValuesPerDoc = 0;
      int index = 0;
      int nextIndex = 0;
     while ((nextIndex = openBitSet.nextSetBit(index + 1)) != -1) {
         if (maxNumValuesPerDoc < (nextIndex - index)) {
             maxNumValuesPerDoc = nextIndex - index;
         }
         index = nextIndex;
     }
}

public void flush(File file) {
    openBitSet.trimTrailingZeros();
    try {
    DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
    
    flush(dataOutputStream);
    } catch (Exception ex) {
        throw new RuntimeException();
    }
    
  }

public void flush(DataOutputStream dataOutputStream) {
    try {
      dataOutputStream.writeInt(startElement);
      dataOutputStream.writeInt(currentSize);
      dataOutputStream.writeInt(openBitSet.getNumWords());
      long[] bits = openBitSet.getBits();
      for (int i = 0; i < openBitSet.getNumWords(); i++) {
        dataOutputStream.writeLong(bits[i]);
      }
      OffHeapCompressedIntArray directMemoryArray = (OffHeapCompressedIntArray) compressedIntArray;
      directMemoryArray.getStorage().rewind();
      int sizeInBytes = (int) OffHeapCompressedIntArray.getRequiredBufferSize(currentSize, numBitsPerElement);
      byte[] bytes = new byte[sizeInBytes];
      directMemoryArray.getStorage().get(bytes);
      dataOutputStream.write(bytes);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      IOUtils.closeQuietly(dataOutputStream);
    }
}

  public static CompressedMultiArrayChunk readFromFile(int numBitsPerElement, File file, ReadMode readMode) {
    CompressedMultiArrayChunk arrayChunk = new CompressedMultiArrayChunk(numBitsPerElement);
    int numWords = 0;
    DataInputStream dataInputStream = null;
    try {
      dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
      arrayChunk.startElement = dataInputStream.readInt();
      arrayChunk.currentSize = dataInputStream.readInt();
       numWords = dataInputStream.readInt();
      long[] words = new long[numWords];
      for (int i = 0; i < words.length; i++) {
        words[i] = dataInputStream.readLong();
      }
      arrayChunk.openBitSet = new OpenBitSet(words, words.length);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      IOUtils.closeQuietly(dataInputStream);
    }
    FileInputStream fileInputStream = null;
    FileChannel channel = null;
    try {
      fileInputStream = new FileInputStream(file);
      channel = fileInputStream.getChannel();
      int offset = 4 + 4 + 4 + numWords * 8;
      int sizeInBytes = (int) OffHeapCompressedIntArray.getRequiredBufferSize(arrayChunk.currentSize, numBitsPerElement);
      ByteBuffer byteBuffer = null;
      if (readMode == ReadMode.MemoryMapped) {
        byteBuffer = channel.map(MapMode.READ_ONLY, offset, sizeInBytes);    
        OffHeapCompressedIntArray compressedIntArray = new OffHeapCompressedIntArray(arrayChunk.currentSize, numBitsPerElement, byteBuffer);
        arrayChunk.compressedIntArray = compressedIntArray;
      } else if (readMode == ReadMode.DirectMemory) {
        byteBuffer = ByteBuffer.allocateDirect(sizeInBytes);
        channel.read(byteBuffer, offset);
        OffHeapCompressedIntArray compressedIntArray = new OffHeapCompressedIntArray(arrayChunk.currentSize, numBitsPerElement, byteBuffer);
        arrayChunk.compressedIntArray = compressedIntArray;
      } else if (readMode == ReadMode.Heap) {
        byteBuffer = channel.map(MapMode.READ_ONLY, offset, sizeInBytes);    
            HeapCompressedIntArray heapCompressedIntArray = new HeapCompressedIntArray(arrayChunk.currentSize, numBitsPerElement);
            for (int i = 0; i < heapCompressedIntArray.getBlocks().length; i++) {
              heapCompressedIntArray.getBlocks()[i] = Bits.getLong(byteBuffer, i);
            }
            arrayChunk.compressedIntArray = heapCompressedIntArray;
      } else {
        throw new UnsupportedOperationException();
      }
     
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      try {
        channel.close();
      } catch (IOException e) {
       logger.error("Could not close the file channel",e);
      }
      IOUtils.closeQuietly(fileInputStream);
    }
    arrayChunk.initSkipLists();
    return arrayChunk;
  }
  public MultiChunkIterator iterator() {
    return new MultiChunkIterator(skipList, offsets, openBitSet, compressedIntArray, startElement, currentSize);
  }
  public int randomRead(int[] buffer, int index) {
      if (skipList.length ==0 || index < skipList[0]) {
          return 0;
      }
      int currentIndex = Arrays.binarySearch(skipList, index);
      if (currentIndex < 0) {
          currentIndex = -(currentIndex + 2);
      }
      int delta = index - skipList[currentIndex];
      int bitSetIndex = offsets[currentIndex];
      while (delta != 0 && bitSetIndex != -1) {
        bitSetIndex = openBitSet.nextSetBit(bitSetIndex + 1);
        if (bitSetIndex == -1) {
          return 0;
        }
        delta--;
      }
     
      int next = openBitSet.nextSetBit(bitSetIndex + 1);
      if (next == -1) {
        next = currentSize;
      }
      int ret = 0;
      int tmp;
      while(bitSetIndex < next) {
        tmp = compressedIntArray.getInt(bitSetIndex);
        if (tmp != 0) {
          buffer[ret] = tmp;        
          ret++;
        }
        bitSetIndex++;
      }
      return ret;
  }
  
  public OpenBitSet getOpenBitSet() {
    return openBitSet;
  }

  public int[] getSkipList() {
    return skipList;
  }

  public int[] getOffsets() {
    return offsets;
  }

  public void setOpenBitSet(OpenBitSet openBitSet) {
    this.openBitSet = openBitSet;
  }

  public void setSkipList(int[] skipList) {
    this.skipList = skipList;
  }

  public void setOffsets(int[] offsets) {
    this.offsets = offsets;
  }

public int getMaxNumValuesPerDoc() {
    return maxNumValuesPerDoc;
}


  
}
