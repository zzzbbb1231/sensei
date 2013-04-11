package com.senseidb.ba.gazelle.persist;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.MetadataAware;
import com.senseidb.ba.gazelle.SegmentMetadata;
import com.senseidb.ba.gazelle.custom.GazelleCustomIndex;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.GazelleUtils;
import com.senseidb.ba.gazelle.utils.StreamUtils;

public class MetadataPersistentManager {
  private static Logger logger = Logger.getLogger(MetadataPersistentManager.class);  
  public static void flush(IndexSegment segment, String basePath, FileSystemMode mode, FileSystem fs) throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl indexSegmentImpl = (GazelleIndexSegmentImpl) segment;
    
    String fileName = basePath + "/" + GazelleUtils.METADATA_FILENAME;
    Path path = new Path(fileName);
    DataOutputStream ds = StreamUtils.getOutputStream(fileName, mode, fs);
    PropertiesConfiguration config = new PropertiesConfiguration();
    try {
      for (String column : indexSegmentImpl.getColumnTypes().keySet()) {
        ColumnMetadata columnMetadata = indexSegmentImpl.getColumnMetadataMap().get(column);       
        if (columnMetadata != null) {
          columnMetadata.addToConfig(config);
        }
      }
      if (indexSegmentImpl.getSegmentMetadata() != null) {
        indexSegmentImpl.getSegmentMetadata().addToConfig(config);
      }
      for (GazelleCustomIndex customIndex : indexSegmentImpl.getCustomIndexes().values()) {
       customIndex.getCreator().saveToProperties(config);
      }
    } finally {
      config.save(ds);
      ds.flush();
      ds.close();
    }
  }

  
  
  public static HashMap<String, ColumnMetadata> readFromFile(PropertiesConfiguration config) {
    HashMap<String, ColumnMetadata> columnMetadataMap = new HashMap<String, ColumnMetadata>();
    Iterator columns = config.getKeys("column");
    while (columns.hasNext()) {
      String key = (String) columns.next();
      String columnName = key.split("\\.")[1];
      ColumnMetadata metadata = ColumnMetadata.valueOf(config, columnName);    
      if (!columnMetadataMap.containsKey(columnName)) {
        columnMetadataMap.put(columnName, metadata);
      }
      
    }
    return columnMetadataMap;
  }
  
  public static SegmentMetadata readSegmentMetadata(PropertiesConfiguration config) throws IllegalAccessException {
    SegmentMetadata segmentMetadata = new SegmentMetadata();
    try {
    Iterator keys = config.getKeys("segment");
    while (keys.hasNext()) {
      String key = keys.next().toString();
      if (!SegmentMetadata.isFromDefaultList(key)) {
        segmentMetadata.put(key, config.getString(key));
      }
    }
    
    if (config.containsKey("segment.aggregation")) {
      segmentMetadata.setAggregationLevel(config.getString("segment.aggregation"));
    }
    
    if (config.containsKey("segment.cluster.name")) {
      segmentMetadata.setClusterName(config.getString("segment.cluster.name"));
    }
    
    if (config.containsKey("segment.endTime")) {
      segmentMetadata.setEndTime(config.getString("segment.endTime"));
    }
    
    if (config.containsKey("segment.startTime")) {
      segmentMetadata.setStartTime(config.getString("segment.startTime"));
    }
    
    if (config.containsKey("segment.time.Type")) {
      segmentMetadata.setTimeType(config.getString("segment.time.Type"));
    }
    if (config.containsKey(SegmentMetadata.SEGMENT_CRC)) {
      segmentMetadata.setCrc(config.getString(SegmentMetadata.SEGMENT_CRC));
    }
    } catch (Exception ex) {
      logger.error(ex.getMessage(), ex);
    }
    return segmentMetadata;
  }
}
