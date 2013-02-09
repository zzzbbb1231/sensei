package com.senseidb.ba;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Arrays;

import junit.framework.Assert;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.util.FileUploadUtils;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.SingleNodeStarter;
@Ignore
public class BASegmentRecoverTest  extends Assert {

  private static ZkManager zkManager;
  

  private static String httpUploadDirectory;
  @AfterClass
  public static void tearDown() throws Exception {
    SingleNodeStarter.shutdown(); 
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    zkManager = new ZkManager("localhost:2181", "testCluster2");
    zkManager.removePartition(0);
    zkManager.removePartition(1);
    FileUtils.deleteDirectory(new File(httpUploadDirectory));
  }
  
  @BeforeClass
  public static void setUp() throws Exception {
    
    ZkClient zkClient = new ZkClient("localhost:2181");
    zkClient.deleteRecursive("/sensei-ba/partitions/testCluster2");    
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    
    File ConfDir1 = new File(BASegmentRecoverTest.class.getClassLoader().getResource("ba-conf").toURI());
     httpUploadDirectory = "/tmp/fileUpload";
    FileUtils.deleteDirectory(new File(httpUploadDirectory));
    new File(httpUploadDirectory).mkdirs();
    //createAndLaunchJettyServer();
    zkManager = new ZkManager("localhost:2181", "testCluster2");
    zkManager.removePartition(0);
    zkManager.removePartition(1);
    SingleNodeStarter.start(ConfDir1, 0);
   
    File corruptedSegment = new File(BASegmentRecoverTest.class.getClassLoader().getResource("testSegments/corruptedSegment1.tar.gz").toURI());
    File normalSegment = new File(BASegmentRecoverTest.class.getClassLoader().getResource("testSegments/segment1.tar.gz").toURI());
    FileInputStream inputStream = new FileInputStream(corruptedSegment);
    FileUploadUtils.sendFile("localhost", "8088", "segment1", inputStream, corruptedSegment.length());
    IOUtils.closeQuietly(inputStream);
     inputStream = new FileInputStream(corruptedSegment);
    FileUploadUtils.sendFile("localhost", "8088", "segment3", inputStream, corruptedSegment.length());
    IOUtils.closeQuietly(inputStream);
    Thread.sleep(1000);    
    inputStream = new FileInputStream(normalSegment);
    FileUploadUtils.sendFile("localhost", "8088", "segment1", new FileInputStream(normalSegment), normalSegment.length());  
    IOUtils.closeQuietly(inputStream);
    inputStream = new FileInputStream(normalSegment);
    FileUploadUtils.sendFile("localhost", "8088", "segment2", new FileInputStream(normalSegment), normalSegment.length());
    IOUtils.closeQuietly(inputStream);
    SingleNodeStarter.waitTillServerStarts(20000);
  }

  


  
  @Test
  public void test1AggregateBQLWithRangePredicate() throws Exception {
    String req = "{\"bql\":\"select *  \"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei"), new JSONObject(req).toString());
   
    assertEquals( 20000, resp.getInt("numhits"));
  }
  /**
   * @throws Exception
   */
  @Test
  public void test2GetValidation() throws Exception {
    
   
    String stringResponse = FileUploadUtils.getStringResponse("http://localhost:8088/validation");
    JSONObject response = new JSONObject(stringResponse);
    //this value is constantly changing
    
    response.remove("currentDelayInDays");
    assertEquals(200, response.getInt("maxTime"));
    assertEquals(50, response.getInt("minTime"));
    assertEquals("[\"101,149\"]", response.getString("missingPeriods"));
    assertEquals("[\"segment3\"]", response.getString("failedSegments"));
    assertEquals("[\"segment2,segment1\"]", response.getString("duplicateSegments"));
    /*assertEquals( "{\n" + 
    		" \"currentDelayInDays\": 15517,\n" + 
    		" \"duplicateSegments\": [\"segment2,segment1\"],\n" + 
    		" \"failedSegments\": [\"segment3\"],\n" + 
    		" \"maxTime\": 200,\n" + 
    		" \"minTime\": 50,\n" + 
    		" \"missingPeriods\": [\"101,149\"],\n" + 
    		" \"overlappingPeriods\": [],\n" + 
    		" \"overlappingSegments\": []\n" + 
    		"}", stringResponse.trim());*/
  }
}
