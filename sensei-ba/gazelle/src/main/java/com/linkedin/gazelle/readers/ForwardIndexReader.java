package com.linkedin.gazelle.readers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.apache.log4j.Logger;

import com.linkedin.gazelle.utils.ColumnMedata;
import com.linkedin.gazelle.utils.CompressedIntArray;
import com.linkedin.gazelle.utils.ReadMode;

public class ForwardIndexReader {

  public static Logger logger = Logger.getLogger(ForwardIndexReader.class);
  public static int count = 0;

  public static CompressedIntArray readForwardIndex(ColumnMedata metadata, File file, ReadMode mode) {
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
    } catch (IOException e) {
      e.printStackTrace();
    }
    return compressedIntArray;
  }
}
