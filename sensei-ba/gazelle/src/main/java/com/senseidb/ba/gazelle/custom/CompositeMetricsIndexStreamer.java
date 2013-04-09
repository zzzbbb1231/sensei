package com.senseidb.ba.gazelle.custom;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;

import com.senseidb.ba.gazelle.utils.Bits;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.HeapCompressedIntArray;
import com.senseidb.ba.gazelle.utils.IntArray;
import com.senseidb.ba.gazelle.utils.OffHeapCompressedIntArray;
import com.senseidb.ba.gazelle.utils.StreamUtils;

public class CompositeMetricsIndexStreamer {
  private HeapCompressedIntArray buffer;
  private final int numberOfMetrics;
  private DataOutputStream ds;
  private int currentBufferIndex = 0;
  private int flushedDocs = 0;
  private final FileSystemMode mode;
  private final FileSystem fs;
  private final String baseDir;

  public CompositeMetricsIndexStreamer(String baseDir, FileSystemMode mode, FileSystem fs, int numberOfDocs, int numberOfMetrics,
      int dictionarySize) {
    this.baseDir = baseDir;
    this.mode = mode;
    this.fs = fs;
    this.numberOfMetrics = numberOfMetrics;
    buffer = new HeapCompressedIntArray(numberOfMetrics * 64, OffHeapCompressedIntArray.getNumOfBits(dictionarySize));
    File baseDirFile = new File(baseDir);
    if (!baseDirFile.exists()) {
      baseDirFile.mkdirs();
    }
  }

  public void addValue(int docId, int metricIndex, int valueId) {
    if (ds == null) {     
      ds = StreamUtils.getOutputStream( baseDir + "/compositeMetricIndexes.fwd", mode, fs);
    }
    
    try {
      currentBufferIndex = docId - flushedDocs;
      if (currentBufferIndex >= 64) {
        for (int i = 0; i < buffer.getBlocks().length; i++) {
          ds.writeLong(buffer.getBlocks()[i]);
          buffer.getBlocks()[i] = 0;
        }
        currentBufferIndex = 0;
        flushedDocs += 64;
      }
      
      int bufferIndex = currentBufferIndex * numberOfMetrics + metricIndex;
      buffer.setInt(bufferIndex, valueId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void flush() {
    try {
      for (int i = 0; i < buffer.getBlocks().length; i++) {
        ds.writeLong(buffer.getBlocks()[i]);
        buffer.getBlocks()[i] = 0;
      }
      ds.flush();
      IOUtils.closeQuietly(ds);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

 /* public  static IntArray getMemoryMapped(String baseDir, int docCount, int metricCount, int dictionaryCount) {
    File processedFile = new File(baseDir, "compositeMetricIndexes_processed.fwd");
    try {
      synchronized(CompositeMetricsIndexStreamer.class) {        
        if (!processedFile.exists()) {
          createProcessedFile(baseDir, docCount, metricCount, dictionaryCount, processedFile);
        }
      }
      RandomAccessFile forwardIndexFile = new RandomAccessFile(processedFile, "r");
      ByteBuffer byteBuffer = forwardIndexFile.getChannel().map(MapMode.READ_ONLY, 0,
          8 * HeapCompressedIntArray.size(docCount * metricCount, OffHeapCompressedIntArray.getNumOfBits(dictionaryCount)));
      OffHeapCompressedIntArray compressedIntArray = new OffHeapCompressedIntArray(docCount * metricCount, OffHeapCompressedIntArray.getNumOfBits(dictionaryCount), byteBuffer);
      return compressedIntArray;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }*/
  public  static IntArray getMemoryMapped(String baseDir, int docCount, int metricCount, int dictionaryCount) {
    try {
      RandomAccessFile forwardIndexFile = new RandomAccessFile(new File(baseDir, "compositeMetricIndexes.fwd"), "r");
      HeapCompressedIntArray rawBasedIndex = null;
      try {
        ByteBuffer byteBuffer = forwardIndexFile.getChannel().map(MapMode.READ_ONLY, 0,
            8 * HeapCompressedIntArray.size(docCount * metricCount, OffHeapCompressedIntArray.getNumOfBits(dictionaryCount)));
        rawBasedIndex = new HeapCompressedIntArray(docCount * metricCount, OffHeapCompressedIntArray.getNumOfBits(dictionaryCount));
        for (int i = 0; i < rawBasedIndex.getBlocks().length; i++) {
          rawBasedIndex.getBlocks()[i] = Bits.getLong(byteBuffer, i);
        }
      } finally {
        forwardIndexFile.getChannel().close();
        forwardIndexFile.close();
      }
      //rotating our matrix to use column major order
      HeapCompressedIntArray processedCompressedIntArray = new HeapCompressedIntArray(docCount * metricCount,
          OffHeapCompressedIntArray.getNumOfBits(dictionaryCount));
      for (int i = 0; i < docCount; i++) {
        for (int j = 0; j < metricCount; j++) {
          processedCompressedIntArray.setInt(j * docCount + i, rawBasedIndex.getInt(i * metricCount + j));
        }
      }
      return processedCompressedIntArray;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  public static void createProcessedFile(String baseDir, int docCount, int metricCount, int dictionaryCount, File processedFile)
      throws FileNotFoundException, IOException {
    RandomAccessFile forwardIndexFile = new RandomAccessFile(new File(baseDir, "compositeMetricIndexes.fwd"), "r");
    HeapCompressedIntArray rawBasedIndex = null;
    try {
      ByteBuffer byteBuffer = forwardIndexFile.getChannel().map(MapMode.READ_ONLY, 0,
          8 * HeapCompressedIntArray.size(docCount * metricCount, OffHeapCompressedIntArray.getNumOfBits(dictionaryCount)));
      rawBasedIndex = new HeapCompressedIntArray(docCount * metricCount, OffHeapCompressedIntArray.getNumOfBits(dictionaryCount));
      for (int i = 0; i < rawBasedIndex.getBlocks().length; i++) {
        rawBasedIndex.getBlocks()[i] = Bits.getLong(byteBuffer, i);
      }
    } finally {
      forwardIndexFile.getChannel().close();
      forwardIndexFile.close();
    }
    //rotating our matrix to use column major order
    HeapCompressedIntArray processedCompressedIntArray = new HeapCompressedIntArray(docCount * metricCount,
        OffHeapCompressedIntArray.getNumOfBits(dictionaryCount));
    for (int i = 0; i < docCount; i++) {
      for (int j = 0; j < metricCount; j++) {
        processedCompressedIntArray.setInt(j * docCount + i, rawBasedIndex.getInt(i * metricCount + j));
      }
    }
    DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(processedFile)));
    try {
      for (int i = 0; i < processedCompressedIntArray.getBlocks().length; i++) {
        dataOutputStream.writeLong(processedCompressedIntArray.getBlocks()[i]);
      }
    } finally {
      IOUtils.closeQuietly(dataOutputStream);
    }
  }
}
