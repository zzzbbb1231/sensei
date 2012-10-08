package com.senseidb.ba.gazelle.utils;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StreamUtils {

  public static InputStream getInputStream(String filePath, FileSystemMode mode ,FileSystem fs) throws IOException {
    InputStream is = null;
    switch(mode) {
      case DISK:
        is = new FileInputStream(new File(filePath));
        break;
      case HDFS:
        is = fs.open(new Path(filePath));
        break;
      default:
        throw new UnsupportedOperationException();
    }
    return is;
  }
  
  public static OutputStream getOutputStream(String filePath, FileSystemMode mode ,FileSystem fs) throws IOException {
    OutputStream is = null;
    switch(mode) {
      case DISK:
        is = new DataOutputStream(new FileOutputStream(new File(filePath)));
        break;
      case HDFS:
        is = fs.create(new Path(filePath));
        break;
      default:
        throw new UnsupportedOperationException();
    }
    return is;
  }
}
