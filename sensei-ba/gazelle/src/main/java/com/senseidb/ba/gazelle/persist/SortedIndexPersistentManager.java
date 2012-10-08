package com.senseidb.ba.gazelle.persist;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.senseidb.ba.gazelle.impl.SortedForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.StreamUtils;

public class SortedIndexPersistentManager {
  public static void flush(String filePath, SortedForwardIndexImpl sortedForwardIndexImpl, FileSystemMode mode, FileSystem fs) throws IOException {
    Path path = new Path(filePath);
    DataOutputStream dataOutputStream = null;
    try {
      dataOutputStream = StreamUtils.getOutputStream(filePath, mode, fs);
      for (int i = 0; i < sortedForwardIndexImpl.getColumnMetadata().getNumberOfDictionaryValues(); i++) {
        dataOutputStream.writeInt(sortedForwardIndexImpl.getMinDocIds()[i]);
        dataOutputStream.writeInt(sortedForwardIndexImpl.getMaxDocIds()[i]);
      }
      dataOutputStream.flush();
    } finally {
      if (dataOutputStream != null) {
        dataOutputStream.close();
      }
    }
  }

  public static void flush(String filePath, SortedForwardIndexImpl sortedForwardIndexImpl, FileSystemMode mode) throws IOException {
    flush(filePath, sortedForwardIndexImpl, mode, null);
  }

  public static void persist(File file, SortedForwardIndexImpl sortedForwardIndexImpl) throws IOException {
    DataOutputStream dataOutputStream = null;
    try {
      dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
      for (int i = 0; i < sortedForwardIndexImpl.getColumnMetadata().getNumberOfDictionaryValues(); i++) {
        dataOutputStream.writeInt(sortedForwardIndexImpl.getMinDocIds()[i]);
        dataOutputStream.writeInt(sortedForwardIndexImpl.getMaxDocIds()[i]);
      }
      dataOutputStream.flush();
    } finally {
      if (dataOutputStream != null) {
        dataOutputStream.close();
      }
    }
  }

  public static void readMinMaxRanges(File file, SortedForwardIndexImpl sortedForwardIndexImpl) throws IOException {
    DataInputStream dataInputStream = null;
    try {
      dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
      for (int i = 0; i < sortedForwardIndexImpl.getColumnMetadata().getNumberOfDictionaryValues(); i++) {
        sortedForwardIndexImpl.getMinDocIds()[i] = dataInputStream.readInt();
        sortedForwardIndexImpl.getMaxDocIds()[i] = dataInputStream.readInt();
      }
    } finally {
      IOUtils.closeQuietly(dataInputStream);
    }
  }
}
