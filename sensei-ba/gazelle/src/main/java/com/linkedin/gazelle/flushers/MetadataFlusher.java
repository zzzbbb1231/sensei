package com.linkedin.gazelle.flushers;

import java.io.File;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.linkedin.gazelle.utils.ColumnMedata;
import com.linkedin.gazelle.utils.GazelleUtils;

public class MetadataFlusher {

  public static void flush(HashMap<String, ColumnMedata> metadataMap, String baseDir) throws ConfigurationException {
    PropertiesConfiguration config = new PropertiesConfiguration(new File(baseDir, GazelleUtils.METADATA_FILENAME));
    try {
      for (String column : metadataMap.keySet()) {
        metadataMap.get(column).addToConfig(config);
      }
    } finally {
      config.save();
    }
  }
}
