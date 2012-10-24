package com.senseidb.ba.gazelle.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
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
        is = new BufferedInputStream(new FileInputStream(new File(filePath)));
        break;
      case HDFS:
        is = new BufferedInputStream(fs.open(new Path(filePath)));
        break;
      default:
        throw new UnsupportedOperationException();
    }
    return is;
  }
  
  public static DataOutputStream getOutputStream(String filePath, FileSystemMode mode ,FileSystem fs) throws IOException {
    DataOutputStream is = null;
    switch(mode) {
      case DISK:
        is = new DataOutputStream(new BufferedOutputStream(new  FileOutputStream(new File(filePath))));
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
