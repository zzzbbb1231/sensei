 package com.senseidb.ba;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

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
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.management.SegmentType;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.SingleNodeStarter;

public class ZkSegmentCreator {
  private static ZkManager zkManager;

  public static void main(String[] args) throws Exception {
    //zkManager = new ZkManager("localhost:2181", "ba-server");
    GazelleIndexSegmentImpl indexSegmentImpl = GenericIndexCreator.create(new File("/Users/vzhabiuk/Downloads/2006.csv"));
   
    File indexDir = new File("testIndex");
    SingleNodeStarter.rmrf(indexDir);
    indexDir.mkdirs();
    SegmentPersistentManager.flushToDisk(indexSegmentImpl, indexDir);
    
  }
  
  public static void main() throws Exception {
    DatumReader<GenericRecord> datumReader =
        new GenericDatumReader<GenericRecord>();
    DataFileStream<GenericRecord> dataFileReader =
        new DataFileStream<GenericRecord>(new BufferedInputStream(new FileInputStream(new File("/tmp/ba-index-standalone/part-1.avro"))), datumReader);
    Schema schema = dataFileReader.getSchema();
    int count = 0;
    int i = 0;
    long time = System.currentTimeMillis();
    while (dataFileReader.hasNext()) {
      if (i == 10000) {
        System.out.println("Time to process 10k elements is " + (System.currentTimeMillis() - time) + ",count = " + count);
        i = 0;
        time = System.currentTimeMillis();
      }
      
      GenericRecord record = dataFileReader.next();
      System.out.println(record.toString());
      count += new JSONObject(record.toString()).hashCode();
      i++;
    }
  }
}
