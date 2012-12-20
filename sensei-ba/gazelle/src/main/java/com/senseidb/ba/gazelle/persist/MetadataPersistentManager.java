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

import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.SegmentMetadata;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.GazelleUtils;
import com.senseidb.ba.gazelle.utils.StreamUtils;

public class MetadataPersistentManager {

  public static void flush(Map<String, ColumnMetadata> metadataMap, SegmentMetadata segmentMetadata, String basePath, FileSystemMode mode, FileSystem fs) throws ConfigurationException, IOException {
    String fileName = basePath + "/" + GazelleUtils.METADATA_FILENAME;
    Path path = new Path(fileName);
    DataOutputStream ds = StreamUtils.getOutputStream(fileName, mode, fs);
    PropertiesConfiguration config = new PropertiesConfiguration();
    try {
      for (String column : metadataMap.keySet()) {
        ColumnMetadata columnMetadata = metadataMap.get(column);       
        columnMetadata.addToConfig(config);
      }
      if (segmentMetadata != null) {
        segmentMetadata.addToConfig(config);
      }
    } finally {
      config.save(ds);
    }
  }

  public static void flush(Map<String, ColumnMetadata> metadataMap, SegmentMetadata segmentMetadataImpl, String basePath, FileSystemMode mode) throws ConfigurationException, IOException {
    flush(metadataMap, segmentMetadataImpl, basePath, mode, null);
  }
  
  public static HashMap<String, ColumnMetadata> readFromFile(PropertiesConfiguration config) {
    HashMap<String, ColumnMetadata> columnMetadataMap = new HashMap<String, ColumnMetadata>();
    Iterator columns = config.getKeys("column");
    while (columns.hasNext()) {
      String key = (String) columns.next();
      String columnName = key.split("\\.")[1];
      ColumnMetadata metadata = new ColumnMetadata();
      metadata.setByteLength(config.getLong("column." + columnName + ".byteLength"));
      metadata.setNumberOfElements(config.getInt("column." + columnName + ".numberOfElements"));
      metadata.setNumberOfDictionaryValues(config.getInt("column." + columnName + ".numberOfDictionaryValues"));
      metadata.setBitsPerElement(config.getInt("column." + columnName + ".bitsPerElement"));
      metadata.setColumnType(ColumnType.valueOfStr(config.getString("column." + columnName + ".columnType")));
      metadata.setSorted(config.getBoolean("column." + columnName + ".sorted"));
      metadata.setMulti(config.getBoolean("column." + columnName + ".multi"));
      metadata.setSecondarySorted(config.getBoolean("column." + columnName + ".secondarySorted", false));
      metadata.setName(columnName);
      if (!columnMetadataMap.containsKey(columnName)) {
        columnMetadataMap.put(columnName, metadata);
      }
      
    }
    return columnMetadataMap;
  }
  
  public static SegmentMetadata readSegmentMetadata(PropertiesConfiguration config) throws IllegalAccessException {
    SegmentMetadata segmentMetadata = new SegmentMetadata();
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

    return segmentMetadata;
  }
}
