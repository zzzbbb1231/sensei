package com.senseidb.ba.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.ba.gazelle.creators.AvroSegmentCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.gazelle.utils.GazelleUtils;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.management.SegmentInfo;
import com.senseidb.ba.management.SegmentType;
import com.senseidb.ba.management.SegmentUtils;
import com.senseidb.ba.management.ZkManager;


public class BaClient {
  public static void main(String[] args11) throws Exception{
    String usage = "Usage:\n" + 
                    "exit -  exits the console\n" + 
                    "print prints directory layout\n"  +
                    "clean <partitionId> <segmentId>- purges the partition's segment\n"  +
                    "add <partition_id> <segmentId> <avroPath|gazelleDirectoryPath>\n";  
    File indexDir = new File("tmp");
    
    //FileUtils.deleteDirectory(indexDir);
    if (!indexDir.exists()) {
      indexDir.mkdirs();
    }
    //eat1-app184.stg.linkedin.com:10000
    ZkManager zkManager = new ZkManager("localhost:2181", "ba-server");
   
    
    //ZkManager zkManager = new ZkManager("eat1-app266.stg.linkedin.com:10000", "adsClickEvents");
    //ZkManager zkManager = new ZkManager("localhost:2121", args11[0]);
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    System.out.println(usage);
    while(true) {
      String command = br.readLine();
      try {
        String[] arguments = command.split(" ");
        if (arguments[0].equalsIgnoreCase("exit")) {
          System.out.println("Client has been shutdown. \nBye!");
          break;
        } else if (arguments[0].equalsIgnoreCase("print")) {          
            System.out.println(printSegments(zkManager).toString(1));
        } else if (arguments[0].equalsIgnoreCase("clean")) {
          int partition = Integer.parseInt(arguments[1]);
          if (arguments.length == 3) {
            String segmentId = arguments[2];
            zkManager.removeSegment(partition, segmentId);
            System.out.println("Segment " + segmentId + " was removed");
          } else {
            zkManager.removePartition(partition);
            System.out.println("Partition " + partition + " was purged from the data");
          }
          continue;
        } else if (arguments[0].equalsIgnoreCase("add")) {
          int partition = Integer.parseInt(arguments[1]);
          String segmentId = arguments[2];
          String path = arguments[3];
          File filePath = new File(path);
          //FileUtils.deleteDirectory(indexDir);
          indexDir.mkdirs();
          if (path.endsWith(".avro")) {
            GazelleIndexSegmentImpl indexSegmentImpl =  AvroSegmentCreator.readFromAvroFile(new File(path));
            File compressedFile = TestUtil.createCompressedSegment(segmentId, indexSegmentImpl, indexDir);
            zkManager.registerSegment(partition, segmentId, compressedFile.getAbsolutePath(), System.currentTimeMillis());
            System.out.println("The segment has been registered");
          } else if (filePath.isDirectory()) {
            if (new File(filePath, GazelleUtils.METADATA_FILENAME).exists()) {
            GazelleIndexSegmentImpl indexSegmentImpl = SegmentPersistentManager.read(filePath, ReadMode.DirectMemory);
            File compressedFile = TestUtil.createCompressedSegment(segmentId, indexSegmentImpl, indexDir);
            zkManager.registerSegment(partition, segmentId, compressedFile.getAbsolutePath(), System.currentTimeMillis());
            System.out.println("The segment has been registered");
            } else {
              File[] avroFiles =  filePath.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                  return pathname.getName().endsWith(".avro");
                }
              });
              System.out.println("There are " + avroFiles.length + " avro files to index");
              for (File avroFile : avroFiles) {
                segmentId = avroFile.getName().substring(0, avroFile.getName().length() - ".avro".length());
                File compressedFile = new File(indexDir, segmentId + ".tar.gz");
                if (!compressedFile.exists() ) {
                GazelleIndexSegmentImpl indexSegmentImpl =  AvroSegmentCreator.readFromAvroFile(avroFile);
                 compressedFile = TestUtil.createCompressedSegment(segmentId, indexSegmentImpl, indexDir);
                 System.out.println("Registered the segment " + segmentId + " containing " + indexSegmentImpl.getLength());
                }
                System.out.println("Registered the segment " + segmentId);
                zkManager.registerSegment(partition, segmentId, compressedFile.getAbsolutePath(), System.currentTimeMillis());
              }
              System.out.println("Done registering segments");
              File[] tarGzFiles =  filePath.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                  return pathname.getName().endsWith(".tar.gz");
                }
              });
              for (File tarGzFile : tarGzFiles) {                
                File compressedFile = tarGzFile;
                segmentId = tarGzFile.getName().substring(0, tarGzFile.getName().indexOf(".tar.gz"));               
                System.out.println("Registered the segment " + segmentId);
                zkManager.registerSegment(partition, segmentId, compressedFile.getAbsolutePath(), System.currentTimeMillis());
              }
              System.out.println("Done registering segments");
              
            }
          } else {
            throw new IllegalStateException("Only gazelle files and gazelle directories are supported");
          }
        } else {
          System.out.println("Only add, clean, exit commands are supported");
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    //FileUtils.deleteDirectory(indexDir);
  }
  public static JSONObject printSegments(ZkManager zkManager) throws JSONException, IOException {
   
      JSONObject obj = new JSONObject();
      for (String partitionIt : zkManager.getPartitions()) {        
        obj.put(partitionIt, getPartitionJson(zkManager, partitionIt));
      }
      return obj;
    }
  public static JSONObject getPartitionJson(ZkManager zkManager, String partitionIt) throws JSONException {
    JSONObject partitionJson = new JSONObject();
    for (String segmentId : zkManager.getSegmentsForPartition(partitionIt)) {
      
      
      SegmentInfo segmentInfo = zkManager.getSegmentInfo(segmentId);
      if (segmentInfo != null) {
        partitionJson.put(segmentId, segmentInfo.toJson());
      }
    }
    return partitionJson;
  }
  
}
