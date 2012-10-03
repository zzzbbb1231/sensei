package com.senseidb.ba.gazelle.flushers;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationException;

import com.senseidb.ba.gazelle.dao.GazelleIndexSegmentImpl;

public class SegmentFlusher {

  public static void flush(GazelleIndexSegmentImpl segment, String baseDir) throws IOException, ConfigurationException {
    DictionaryFlusher.flush(segment.getColumnMetatdaMap(), segment.getTermValueListMap(), baseDir);
    MetadataFlusher.flush(segment.getColumnMetatdaMap(), baseDir);
    HashMap<String, Integer> dictionarySizeMap = new HashMap<String, Integer>();
    for (String column : segment.getTermValueListMap().keySet()) {
      dictionarySizeMap.put(column, segment.getDictionary(column).size());
    }
    ForwardIndexFlusher.flush(segment.getCompressedIntArrayMap(), dictionarySizeMap, segment.getLength(), baseDir);
  }
}
