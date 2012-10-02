package com.linkedin.gazelle.flushers;

import java.io.IOException;

import org.apache.commons.configuration.ConfigurationException;

import com.linkedin.gazelle.dao.GazelleIndexSegmentImpl;

public class SegmentFlusher {

  public static void flush(GazelleIndexSegmentImpl segment, String baseDir) throws IOException, ConfigurationException {
    DictionaryFlusher.flush(segment.getColumnMetatdaMap(), segment.getTermValueListMap(), baseDir);
    MetadataFlusher.flush(segment.getColumnMetatdaMap(), baseDir);
    ForwardIndexFlusher.flush(segment.getForwardIndexMap(), segment.getTermValueListMap(), segment.getLength(), baseDir);
  }
}
