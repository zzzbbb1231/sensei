package com.senseidb.ba.gazelle.persist;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.mortbay.io.RuntimeIOException;

import com.senseidb.ba.ColumnMetadata;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.gazelle.utils.GazelleUtils;
import com.senseidb.ba.gazelle.utils.ReadMode;

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
        forwardIndexFile.getChannel().read(byteBuffer, metadata.getStartOffset());
        break;
      case MMAPPED:
        byteBuffer =
            forwardIndexFile.getChannel().map(MapMode.READ_ONLY, metadata.getStartOffset(), metadata.getByteLength());
        break;
      default:
        throw new UnsupportedOperationException();
      }
      compressedIntArray =
          new CompressedIntArray(metadata.getNumberOfElements(), CompressedIntArray.getNumOfBits(metadata
              .getNumberOfDictionaryValues()), byteBuffer);
    } catch (FileNotFoundException e) {
      logger.error(e);
      throw new RuntimeIOException(e);
    } catch (IOException e) {
      logger.error(e);
      throw new RuntimeIOException(e);
    }
    return compressedIntArray;
  }
  
  public static void flushOnHadoop(Collection<GazelleForwardIndexImpl> forwardIndexes, String basePath, FileSystem fs) throws IOException {
    Path file = new Path(basePath + "/" + GazelleUtils.INDEX_FILENAME);
    FSDataOutputStream ds = fs.create(file);
    List<Long> sortedOffsetList = new ArrayList<Long>();
    try {
      int count = 0;
      for(GazelleForwardIndexImpl gazelleForwardIndexImpl : forwardIndexes) { 
        sortedOffsetList.add(count, new Long(gazelleForwardIndexImpl.getColumnMetadata().getStartOffset()));
        count++;
      }
      Collections.sort(sortedOffsetList);
      for (Long offset : sortedOffsetList) {
        for(GazelleForwardIndexImpl gazelleForwardIndexImpl : forwardIndexes) { 
          if (gazelleForwardIndexImpl.getColumnMetadata().getStartOffset() == offset.longValue()) {
            gazelleForwardIndexImpl.getCompressedIntArray().getStorage().rewind();
            ds.write(gazelleForwardIndexImpl.getCompressedIntArray().getStorage().array());
          }
        }
      }
    } finally {
      ds.close();
    }
  }

  public static void flush(Collection<GazelleForwardIndexImpl> forwardIndexes, File baseDir) throws IOException {
    File file = new File(baseDir, GazelleUtils.INDEX_FILENAME);
    RandomAccessFile fIdxFile = new RandomAccessFile(file, "rw");
    try {
      for(GazelleForwardIndexImpl gazelleForwardIndexImpl : forwardIndexes) { 
        gazelleForwardIndexImpl.getCompressedIntArray().getStorage().rewind();
        fIdxFile.getChannel().write(gazelleForwardIndexImpl.getCompressedIntArray().getStorage(), gazelleForwardIndexImpl.getColumnMetadata().getStartOffset());
        fIdxFile.getChannel().force(true);
      }
    } finally {
      fIdxFile.getChannel().close();
    }
  }
}
