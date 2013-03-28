package com.senseidb.ba.gazelle.custom;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;

import com.senseidb.ba.gazelle.utils.FileSystemMode;

public interface GazelleCustomIndexCreator  {
  public void addValueToDictionary(String column, Object value);
  public void init(String baseDir, FileSystemMode mode, FileSystem fs);
  public void buildDictionary();
  public void addToForwardIndex(String column, Object value);
  public void newDocumentOnForwardIndex(int docId);
  public void saveToProperties(PropertiesConfiguration configToBeAppended);
  public void createForwardIndex();
  public void flushIndex(String baseDir, FileSystemMode mode, FileSystem fs);
}
