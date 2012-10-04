package com.senseidb.ba.gazelle.persist;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.senseidb.ba.ColumnMetadata;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.gazelle.utils.GazelleUtils;

public class MetadataPersistentManager {

  public static void flush(Map<String, ColumnMetadata> metadataMap, File baseDir) throws ConfigurationException {
    PropertiesConfiguration config = new PropertiesConfiguration(new File(baseDir, GazelleUtils.METADATA_FILENAME));
    try {
      for (String column : metadataMap.keySet()) {
        metadataMap.get(column).addToConfig(config);
      }
    } finally {
      config.save();
    }
  }
  public static HashMap<String, ColumnMetadata> readFromFile(PropertiesConfiguration config) {
    HashMap<String, ColumnMetadata> columnMetadataMap = new HashMap<String, ColumnMetadata>();
    Iterator columns = config.getKeys("column");
    while (columns.hasNext()) {
       String key = (String) columns.next();
       String columnName = key.split("\\.")[1];
       ColumnMetadata metadata = new ColumnMetadata();
       metadata.setStartOffset(config.getLong("column." + columnName + ".startOffset"));
       metadata.setByteLength(config.getLong("column." + columnName + ".byteLength"));
       metadata.setNumberOfElements(config.getInt("column." + columnName + ".numberOfElements"));
       metadata.setNumberOfDictionaryValues(config.getInt("column." + columnName + ".numberOfDictionaryValues"));
       metadata.setBitsPerElement(config.getInt("column." + columnName + ".bitsPerElement"));
       metadata.setColumnType(ColumnType.valueOfStr(config.getString("column." + columnName + ".columnType")));
       metadata.setSorted(config.getBoolean("column." + columnName + ".sorted"));
       metadata.setName(columnName);
       if (!columnMetadataMap.containsKey(columnName)) {
         columnMetadataMap.put(columnName, metadata);
       }
    }
    return columnMetadataMap;
  }
}
