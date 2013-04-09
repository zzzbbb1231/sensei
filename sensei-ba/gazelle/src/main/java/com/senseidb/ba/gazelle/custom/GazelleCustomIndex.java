package com.senseidb.ba.gazelle.custom;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;

import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.InvertedIndex;
import com.senseidb.ba.gazelle.SingleValueRandomReader;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.ReadMode;

public interface GazelleCustomIndex {
  public void init(Map<String, ColumnMetadata> properties);  
  public GazelleCustomIndexCreator getCreator();
  public void load(File baseDir, ReadMode readMode);
  public FacetHandler getFacetHandler(String column);
  
  public SingleValueRandomReader getReader(String column);
  public ForwardIndex getForwardIndex(String column);
  public TermValueList getDictionary(String column); 
  public InvertedIndex getInvertedIndex(String column);
  public ColumnType getColumnType(String column);
}
