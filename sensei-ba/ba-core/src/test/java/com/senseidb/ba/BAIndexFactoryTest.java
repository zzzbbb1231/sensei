package com.senseidb.ba;

import java.io.File;

import junit.framework.TestCase;

import org.I0Itec.zkclient.ZkClient;

import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.management.BaIndexFactory;
import com.senseidb.ba.management.SegmentType;
import com.senseidb.ba.management.ZkManager;
import com.senseidb.ba.plugins.ZeusIndexReaderDecorator;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.ba.util.Wait;
import com.senseidb.util.SingleNodeStarter;

public class BAIndexFactoryTest extends TestCase {
  private ZkClient zkClient;
  private BaIndexFactory baIndexFactory;
  private File indexDir;
  private GazelleIndexSegmentImpl indexSegment;
  private ZkManager zkManager;
  
  public void setUp() throws Exception {
     indexDir = new File("tempIndex"); 
    SingleNodeStarter.rmrf(indexDir);
    indexDir.mkdir();
     zkClient = new ZkClient("localhost:2181");
     zkClient.deleteRecursive("/sensei-ba/testCluster2"); 
     zkManager = new ZkManager(zkClient, "testCluster2");
     zkManager.removePartition(0);
     baIndexFactory = new BaIndexFactory(indexDir, "testCluster2", new ZeusIndexReaderDecorator(), zkClient, null, ReadMode.Heap, 0, 0, null);
     baIndexFactory.start();
     indexSegment = TestUtil.createIndexSegment();
  }
 
  public void tearDown() throws Exception {
    baIndexFactory.shutdown();
    SingleNodeStarter.rmrf(indexDir);
    zkClient.unsubscribeAll();
    zkManager.removePartition(0);
    zkClient.close();
  }
  
  public void test1RegisteringTwoSegments() throws Exception {
    File createCompressedSegment = TestUtil.createCompressedSegment("segment1", indexSegment, indexDir);
    zkManager.registerSegment(0, "segment1", createCompressedSegment.getAbsolutePath(),System.currentTimeMillis());
    new Wait(){
      public boolean until() {return baIndexFactory.getIndexReaders().size() == 1;};
    }; 
    File createCompressedSegment2 = TestUtil.createCompressedSegment("segment2", indexSegment, indexDir);
    zkManager.registerSegment(0, "segment2", createCompressedSegment2.getAbsolutePath(),System.currentTimeMillis());
    new Wait(){
      public boolean until() {return baIndexFactory.getIndexReaders().size() == 2;};
    }; 
   
  }
  public void test2RegisteringTwoSegmentsAndRestartingFactory() throws Exception {
    test1RegisteringTwoSegments();
    baIndexFactory.shutdown();
    baIndexFactory = new BaIndexFactory(indexDir,"testCluster2", new ZeusIndexReaderDecorator(), zkClient, null, ReadMode.DirectMemory, 0, 0, null);
    baIndexFactory.start();
    new Wait(){
      public boolean until() {return baIndexFactory.getIndexReaders().size() == 2;};
    }; 
  }
  public void test3Delete() throws Exception {
    test1RegisteringTwoSegments();
    baIndexFactory.shutdown();
    baIndexFactory = new BaIndexFactory(indexDir, "testCluster2", new ZeusIndexReaderDecorator(), zkClient, null, ReadMode.DirectMemory, 0, 0, null);
    baIndexFactory.start();
    zkManager.removeSegment(0, "segment2");
    new Wait() {
      public boolean until() {return baIndexFactory.getSegmentTracker().getIndexReadersWithNoCounts().size() == 1;};
    }; 
  }
}

