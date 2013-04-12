

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;

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
public class BAProdTroubleShootingTest  extends Assert {

  private static ZkManager zkManager;
  private static File indexDir;
  private static File compressedSegment;
  private static GazelleIndexSegmentImpl indexSegmentImpl;
  private static String httpUploadDirectory;
  @AfterClass
  public static void tearDown() throws Exception {
    SingleNodeStarter.shutdown(); 
    SingleNodeStarter.rmrf(new File("ba-index"));
    SingleNodeStarter.rmrf(indexDir);
    FileUtils.deleteDirectory(new File(httpUploadDirectory));
  }
  
  @BeforeClass
  public static void setUp() throws Exception {
    //System.setProperty("com.linkedin.norbert.disableJMX", "true");
    indexDir = new File("testIndex");
    ZkClient zkClient = new ZkClient("localhost:2181");
    zkClient.deleteRecursive("/sensei-ba/partitions/csapBetaEvents");    
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    SingleNodeStarter.rmrf(indexDir);
    File ConfDir1 = new File(BAProdTroubleShootingTest.class.getClassLoader().getResource("ba-conf-temp").toURI());
     httpUploadDirectory = "/tmp/fileUpload";
    FileUtils.deleteDirectory(new File(httpUploadDirectory));
    new File(httpUploadDirectory).mkdirs();
    //createAndLaunchJettyServer();
    zkManager = new ZkManager("localhost:2181", "csapBetaEvents");
    zkManager.removePartition(0);
    zkManager.removePartition(1);
    SingleNodeStarter.start(ConfDir1, 0);
    indexDir.mkdir();
    File file = new File("/home/vzhabiuk/work/tmp/tesla/2013-04-02-22-33-30_daily_0.tar.gz");
    FileInputStream inputStream = new FileInputStream(file);
      FileUploadUtils.sendFile("localhost", "8088", "segment1", inputStream, file.length());
      IOUtils.closeQuietly(inputStream);
    
    SingleNodeStarter.waitTillServerStarts(20000);
  }



  
  @Test
  public void test1() throws Exception {
    String req = "{\"bql\":\"select  Count(*) where accountId = 317970 group by source top 2 limit 0\"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8075/sensei/"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    //assertEquals( 4722, resp.getInt("numhits"));
   // assertEquals( 11954, resp.getJSONObject("mapReduceResult").getInt("sum"));
  }
  @Test
  public void test2() throws Exception {
    Thread.sleep(5000000);
  }
}
