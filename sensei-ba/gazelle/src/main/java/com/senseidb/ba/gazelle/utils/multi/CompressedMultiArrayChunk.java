package com.senseidb.ba.gazelle.utils.multi;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.OpenBitSet;
import org.springframework.util.Assert;

import com.senseidb.ba.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.gazelle.utils.ReadMode;

public class CompressedMultiArrayChunk {
  private OpenBitSet openBitSet;
  private CompressedIntArray compressedIntArray;
  private int startElement;
  private int currentSize = 0;
  private final int numBitsPerElement;
  private int[] skipList;
  private int[] offsets;
  private static final int[] ARRAY_WITH_SINGLE_ZERO = new int[] {0};
  public CompressedMultiArrayChunk(int numBitsPerElement) {
    this.numBitsPerElement = numBitsPerElement;
  }

  public CompressedMultiArrayChunk(int startElement, int numBitsPerElement, int initialSize) {
    this.startElement = startElement;
    this.numBitsPerElement = numBitsPerElement;
    compressedIntArray = new CompressedIntArray(initialSize, numBitsPerElement);
    openBitSet = new OpenBitSet(initialSize);
  }

  public void add(int... values) {
    if (values.length == 0) {
      values = ARRAY_WITH_SINGLE_ZERO;
    }
    Assert.state(skipList == null);
    ensureCapacity(currentSize + values.length);
    openBitSet.set(currentSize);
    for (int i = 0; i < values.length; i++) {
      compressedIntArray.addInt(currentSize + i, values[i]);
    }
    currentSize += values.length;
  }

  private void ensureCapacity(int newCapacity) {
    if (compressedIntArray.getCapacity() < newCapacity) {
      long capacityToSet = Math.max((long) compressedIntArray.getCapacity() * 2, newCapacity);
      Assert.state(CompressedIntArray.getRequiredBufferSize(capacityToSet, numBitsPerElement) <= Integer.MAX_VALUE);
      CompressedIntArray newCompressedArray = new CompressedIntArray((int) capacityToSet, compressedIntArray.getNumOfBitsPerElement());
      compressedIntArray.getStorage().rewind();
      newCompressedArray.getStorage().rewind();
      newCompressedArray.getStorage().put(compressedIntArray.getStorage());
      compressedIntArray = newCompressedArray;
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
  }

  public void flush(File file) {
    openBitSet.trimTrailingZeros();
    DataOutputStream dataOutputStream = null;
    try {
      dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
      dataOutputStream.writeInt(startElement);
      dataOutputStream.writeInt(currentSize);
      dataOutputStream.writeInt(openBitSet.getNumWords());
      long[] bits = openBitSet.getBits();
      for (int i = 0; i < openBitSet.getNumWords(); i++) {
        dataOutputStream.writeLong(bits[i]);
      }
      compressedIntArray.getStorage().rewind();
      int sizeInBytes = (int) CompressedIntArray.getRequiredBufferSize(currentSize, numBitsPerElement);
      byte[] bytes = new byte[sizeInBytes];
      compressedIntArray.getStorage().get(bytes);
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
      int sizeInBytes = (int) CompressedIntArray.getRequiredBufferSize(arrayChunk.currentSize, numBitsPerElement);
      ByteBuffer byteBuffer = null;
      if (readMode == ReadMode.MMAPPED) {
        byteBuffer = channel.map(MapMode.READ_ONLY, offset, sizeInBytes);        
      } else if (readMode == ReadMode.DBBuffer) {
        byteBuffer = ByteBuffer.allocateDirect(sizeInBytes);
        channel.read(byteBuffer, offset);
      } else {
        throw new UnsupportedOperationException();
      }
      CompressedIntArray compressedIntArray = new CompressedIntArray(arrayChunk.currentSize, numBitsPerElement, byteBuffer);
      arrayChunk.compressedIntArray = compressedIntArray;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      IOUtils.closeQuietly(channel);
      IOUtils.closeQuietly(fileInputStream);
    }
    arrayChunk.initSkipLists();
    return arrayChunk;
  }
  public MultiChunkIterator iterator() {
    return new MultiChunkIterator(skipList, offsets, openBitSet, compressedIntArray, startElement, currentSize);
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
  
}
