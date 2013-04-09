 package com.senseidb.ba;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.json.JSONObject;

import com.senseidb.ba.format.GenericIndexCreator;
import com.senseidb.ba.gazelle.creators.AvroSegmentCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.management.SegmentType;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.SingleNodeStarter;

public class ZkSegmentCreator {
  private static ZkManager zkManager;

  public static void main() throws Exception {
    //zkManager = new ZkManager("localhost:2181", "ba-server");
    GazelleIndexSegmentImpl indexSegmentImpl = GenericIndexCreator.create(new File("/tmp/pinot-senseidb/1987.csv"));
   
    File indexDir = new File("testIndex");
    SingleNodeStarter.rmrf(indexDir);
    indexDir.mkdirs();
    SegmentPersistentManager.flushToDisk(indexSegmentImpl, indexDir);
    
  }
  
  public static void main(String[] args) throws Exception {
    AvroSegmentCreator.flushSegmentFromAvroFileWithCompositeMetrics(new AvroSegmentCreator.CreateSegmentInfo().setAvroSegment(new File("/home/vzhabiuk/work/tmp/tesla/tesla.avro")).setOutputDirInfo("/home/vzhabiuk/work/tmp/tesla/serializaed",FileSystemMode.DISK,null).setCompositeMetrics(Arrays.asList("metric*")) );
    
  }
}
