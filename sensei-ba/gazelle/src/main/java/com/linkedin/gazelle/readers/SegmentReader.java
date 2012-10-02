package com.linkedin.gazelle.readers;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.dao.GazelleIndexSegmentImpl;
import com.linkedin.gazelle.utils.GazelleColumnMedata;
import com.linkedin.gazelle.utils.CompressedIntArray;
import com.linkedin.gazelle.utils.GazelleUtils;
import com.linkedin.gazelle.utils.ReadMode;

public class SegmentReader {
  private static Logger logger = Logger.getLogger(SegmentReader.class);

  public static GazelleIndexSegmentImpl read(File indexDir, ReadMode mode) throws ConfigurationException, IOException {
    File file = new File(indexDir, GazelleUtils.METADATA_FILENAME);
    HashMap<String, GazelleColumnMedata> metadataMap = GazelleColumnMedata.readFromFile(new PropertiesConfiguration(file));
    return new GazelleIndexSegmentImpl(metadataMap, getCompressedIntArrayMap(metadataMap, indexDir, mode),
        getTermValueListMap(metadataMap, indexDir));
  }

  private static HashMap<String, CompressedIntArray> getCompressedIntArrayMap(HashMap<String, GazelleColumnMedata> metadataMap, File indexDir, ReadMode mode) {
    HashMap<String, CompressedIntArray> compressedIntArrayMap = new HashMap<String, CompressedIntArray>();
    for (String column : metadataMap.keySet()) {
      File file = new File(indexDir, GazelleUtils.INDEX_FILENAME);
      compressedIntArrayMap.put(column, ForwardIndexReader.readForwardIndex(metadataMap.get(column), file, mode));
    }
    return compressedIntArrayMap;
  }

  private static HashMap<String, TermValueList> getTermValueListMap(HashMap<String, GazelleColumnMedata> metadataMap, File indexDir) throws IOException {
    HashMap<String, TermValueList> dictionaryMap = new HashMap<String, TermValueList>();
    for (String column : metadataMap.keySet()) {
      File file = new File(indexDir, (column + ".dict"));
      TermValueList list =
          DictionaryReader.read(file, metadataMap.get(column).getColumnType(), metadataMap.get(column)
              .getNumberOfDictionaryValues());
      dictionaryMap.put(column, list);
    }
    return dictionaryMap;
  }
}
