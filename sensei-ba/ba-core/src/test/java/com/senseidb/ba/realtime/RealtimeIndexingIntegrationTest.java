package com.senseidb.ba.realtime;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.senseidb.ba.BASentinelTest;
import com.senseidb.util.SingleNodeStarter;

public class RealtimeIndexingIntegrationTest extends BASentinelTest {
  @BeforeClass
  public static void setUp() throws Exception {
    File ConfDir1 = new File(BASentinelTest.class.getClassLoader().getResource("ba-realtime-conf").toURI());
    FileUtils.deleteDirectory(new File("ba-index/ba-data"));
    SingleNodeStarter.start(ConfDir1, 20000);
  }
  public void test9ManagementBackend() throws Exception {
    //Thread.sleep(Integer.MAX_VALUE);
  }
  @AfterClass
  public static void tearDown() throws Exception {
    //Thread.sleep(5000);
    SingleNodeStarter.shutdown(); 
    //SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    FileUtils.deleteDirectory(new File("ba-index/ba-data"));
  }
  public void test4RangeQueryOnMultivaluedColumn() throws Exception {
    
  }
  public void test19TestFederatedBroker() throws Exception {
    
  }
}
