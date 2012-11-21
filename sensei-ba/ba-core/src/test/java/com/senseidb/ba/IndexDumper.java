package com.senseidb.ba;

import java.io.File;

import org.apache.commons.io.FileUtils;

import com.senseidb.ba.format.GenericIndexCreator;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.gazelle.utils.ReadMode;

public class IndexDumper {
public static void main(String[] args) throws Exception {
  GazelleIndexSegmentImpl segment = SegmentPersistentManager.read(new File("/home/vzhabiuk/Downloads/segment0"), ReadMode.DirectMemory);
  //GazelleIndexSegmentImpl segment = GenericIndexCreator.create(new File("/tmp/avroFilesAdsClickYearrly/part-1.avro"));
  System.out.println(segment.getForwardIndex("campaignId"));
  FileUtils.deleteDirectory(new File("nonSortedSegment"));
  FileUtils.deleteDirectory(new File("segment"));
  SegmentPersistentManager.flushToDisk(segment, new File("nonSortedSegment"));
  segment = GenericIndexCreator.create(segment, new String[] {"advertiserId", "campaignId", "creativeId", "time", "memberRegion"});
  SegmentPersistentManager.flushToDisk(segment, new File("segment"));
  
  for (String column : segment.getColumnTypes().keySet()) {
    ForwardIndex forwardIndex = segment.getForwardIndex(column);
    if (forwardIndex instanceof SingleValueForwardIndex) {
    System.out.print(", " +  column);
    }
  }
  System.out.println();
  for(int i = 0; i < 1000; i++) {
    for (String column : segment.getColumnTypes().keySet()) {
      ForwardIndex forwardIndex = segment.getForwardIndex(column);
      if (forwardIndex instanceof SingleValueForwardIndex) {
        System.out.print(", " + forwardIndex.getDictionary().get(((SingleValueForwardIndex)forwardIndex).getValueIndex(i)));
      }
    }
    System.out.println();
  }
  
}
}
