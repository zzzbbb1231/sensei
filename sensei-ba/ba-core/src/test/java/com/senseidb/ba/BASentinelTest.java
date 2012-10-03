package com.senseidb.ba;

import java.io.File;
import java.net.URL;

import junit.framework.TestCase;

import org.json.JSONObject;

import com.senseidb.ba.gazelle.dao.GazelleIndexSegmentImpl;
import com.senseidb.ba.management.SegmentType;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.SingleNodeStarter;

public class BASentinelTest  extends TestCase {

  private ZkManager zkManager;
  private File indexDir;
  private File compressedSegment;
  private GazelleIndexSegmentImpl indexSegmentImpl;
  @Override
  protected void tearDown() throws Exception {
    SingleNodeStarter.shutdown(); 
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    SingleNodeStarter.rmrf(new File("testIndex"));
  }
  @Override
  protected void setUp() throws Exception {    
    indexDir = new File("testIndex");
    SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    SingleNodeStarter.rmrf(indexDir);
    File ConfDir1 = new File(BASentinelTest.class.getClassLoader().getResource("ba-conf").toURI());
    
    zkManager = new ZkManager("localhost:2181");
    zkManager.removePartition(0);
    zkManager.removePartition(1);
    SingleNodeStarter.start(ConfDir1, 0);
    
    indexDir.mkdir();
    indexSegmentImpl = TestUtil.createIndexSegment();
   

  }
  public void test1Init10SegmentsAndDoTheQuery() throws Exception {
    for (int i = 0; i < 2; i ++) {
      File compressedFile = TestUtil.createCompressedSegment("segment" + i, indexSegmentImpl, indexDir);
      zkManager.registerSegment(i % 2, "segment" + i, compressedFile.getAbsolutePath(), SegmentType.COMPRESSED_GAZELLE, System.currentTimeMillis(), Long.MAX_VALUE);
      
    } 
    SingleNodeStarter.waitTillServerStarts(20000);
    String req = "{" + 
        "  " + 
        "    \"from\": 0," + 
        "    \"size\": 10,\n" + 
        "    \"selections\": [" + 
        "    {" + 
        "        \"terms\": {" + 
        "            \"dim_memberGender\": {" + 
        "                \"values\": [\"m\"]," + 
        "                \"excludes\": []," + 
        "                \"operator\": \"or\"" + 
        "            }" + 
        "        }" + 
        "    }" + 
        "   ], \"sort\": [{\"dim_memberCompany\": \"desc\"}]" +
       
        "    }" + 
        "}";
      
     JSONObject resp = null;
     for (int i = 0; i < 2; i ++) {
       resp = TestUtil.search(new URL("http://localhost:8076/sensei"), new JSONObject(req).toString());
     }
     System.out.println(resp.toString(1));
     assertEquals("numhits is wrong", 13224, resp.getInt("numhits"));
}
  
}
