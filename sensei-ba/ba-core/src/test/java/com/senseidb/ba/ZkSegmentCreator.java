package com.senseidb.ba;

import java.io.File;

import com.senseidb.ba.gazelle.creators.SegmentCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.management.SegmentType;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.SingleNodeStarter;

public class ZkSegmentCreator {
  private static ZkManager zkManager;

  public static void main(String[] args) throws Exception {
    zkManager = new ZkManager("localhost:2181", "ba-server");
    GazelleIndexSegmentImpl indexSegmentImpl = new SegmentCreator().readFromAvroFile(new File("/tmp/data/-part-1.avro"));
   
    File indexDir = new File("testIndex");
    SingleNodeStarter.rmrf(indexDir);
    indexDir.mkdirs();
    SegmentPersistentManager.flush(indexSegmentImpl, indexDir);
  }
}
