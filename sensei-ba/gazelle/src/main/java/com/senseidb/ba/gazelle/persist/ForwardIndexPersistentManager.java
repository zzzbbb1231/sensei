package com.senseidb.ba.gazelle.persist;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.Bits;
import com.senseidb.ba.gazelle.utils.OffHeapCompressedIntArray;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.HeapCompressedIntArray;
import com.senseidb.ba.gazelle.utils.IntArray;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.gazelle.utils.StreamUtils;

public class ForwardIndexPersistentManager {
  public static Logger logger = Logger.getLogger(ForwardIndexPersistentManager.class);
  public static int count = 0;

  public static IntArray readForwardIndex(ColumnMetadata metadata, File file, ReadMode mode) {
    count++;
    OffHeapCompressedIntArray compressedIntArray = null;
    RandomAccessFile forwardIndexFile;
    ByteBuffer byteBuffer = null;
    try {
      forwardIndexFile = new RandomAccessFile(file, "r");
      switch (mode) {
      case DirectMemory:
        byteBuffer =  ByteBuffer.allocateDirect((int)metadata.getByteLength());
        forwardIndexFile.getChannel().read(byteBuffer);
        break;
      case MemoryMapped:
        byteBuffer =
            forwardIndexFile.getChannel().map(MapMode.READ_ONLY, 0, metadata.getByteLength());
        break;
      case Heap: 
        byteBuffer =
        forwardIndexFile.getChannel().map(MapMode.READ_ONLY, 0, metadata.getByteLength());
        HeapCompressedIntArray heapCompressedIntArray = new HeapCompressedIntArray(metadata.getNumberOfElements(), OffHeapCompressedIntArray.getNumOfBits(metadata
              .getNumberOfDictionaryValues()));
        for (int i = 0; i < heapCompressedIntArray.getBlocks().length; i++) {
          heapCompressedIntArray.getBlocks()[i] = Bits.getLong(byteBuffer, i);
        }
        forwardIndexFile.getChannel().close();
        forwardIndexFile.close();
        return heapCompressedIntArray;       
       
      default:
        throw new UnsupportedOperationException();
      }
      compressedIntArray =
          new OffHeapCompressedIntArray(metadata.getNumberOfElements(), OffHeapCompressedIntArray.getNumOfBits(metadata
              .getNumberOfDictionaryValues()), byteBuffer);
    } catch (Exception e) {
      logger.error(e);
      throw new RuntimeException(e);
    }
    return compressedIntArray;
  }
  
  public static void flush(GazelleForwardIndexImpl forwardIndex, String basePath, FileSystemMode mode, FileSystem fs) throws IOException {
    String filePath = basePath + "/" + forwardIndex.getColumnMetadata().getName() + ".fwd";
    DataOutputStream ds = StreamUtils.getOutputStream(filePath, mode, fs);
    try {
           IntArray compressedIntArray = forwardIndex.getCompressedIntArray();
           if (compressedIntArray instanceof OffHeapCompressedIntArray) {
             OffHeapCompressedIntArray array = (OffHeapCompressedIntArray) compressedIntArray;
             array.getStorage().rewind();
              byte[] bytes = new byte[(int)forwardIndex.getColumnMetadata().getByteLength()];
              array.getStorage().get(bytes);
              ds.write(bytes);
           } else {
             HeapCompressedIntArray array = (HeapCompressedIntArray) compressedIntArray;
             for (int i = 0; i < array.getBlocks().length; i++) {
               ds.writeLong(array.getBlocks()[i]);
             }
           }
    } finally {
      ds.close();
    }
  }

  public static void flush(Collection<GazelleForwardIndexImpl> forwardIndexes, String basePath) throws IOException {
    for (GazelleForwardIndexImpl gazelleForwardIndexImpl : forwardIndexes) {
        flush(gazelleForwardIndexImpl, basePath, FileSystemMode.DISK, null);
    }
      
  }
}
