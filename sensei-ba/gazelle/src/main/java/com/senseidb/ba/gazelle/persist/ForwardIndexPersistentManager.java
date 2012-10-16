package com.senseidb.ba.gazelle.persist;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.senseidb.ba.ColumnMetadata;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.gazelle.utils.StreamUtils;

public class ForwardIndexPersistentManager {
  public static Logger logger = Logger.getLogger(ForwardIndexPersistentManager.class);
  public static int count = 0;

  public static CompressedIntArray readForwardIndex(ColumnMetadata metadata, File file, ReadMode mode) {
    count++;
    CompressedIntArray compressedIntArray = null;
    RandomAccessFile forwardIndexFile;
    ByteBuffer byteBuffer = null;
    try {
      forwardIndexFile = new RandomAccessFile(file, "r");
      switch (mode) {
      case DBBuffer:
        byteBuffer =  ByteBuffer.allocateDirect((int)metadata.getByteLength());
        forwardIndexFile.getChannel().read(byteBuffer);
        break;
      case MMAPPED:
        byteBuffer =
            forwardIndexFile.getChannel().map(MapMode.READ_ONLY, 0, metadata.getByteLength());
        break;
      default:
        throw new UnsupportedOperationException();
      }
      compressedIntArray =
          new CompressedIntArray(metadata.getNumberOfElements(), CompressedIntArray.getNumOfBits(metadata
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
    List<Long> sortedOffsetList = new ArrayList<Long>();
    try {
          forwardIndex.getCompressedIntArray().getStorage().rewind();
            ds.write(forwardIndex.getCompressedIntArray().getStorage().array());
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
