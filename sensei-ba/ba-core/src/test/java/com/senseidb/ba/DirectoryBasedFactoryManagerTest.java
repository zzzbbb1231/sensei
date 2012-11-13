package com.senseidb.ba;

import java.io.File;

import junit.framework.TestCase;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;

import com.senseidb.ba.gazelle.creators.AvroSegmentCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.management.directory.DirectoryBasedFactoryManager;
import com.senseidb.ba.management.directory.MapBasedIndexFactory;
import com.senseidb.ba.util.TestUtil;
import com.senseidb.ba.util.Wait;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.util.SingleNodeStarter;

public class DirectoryBasedFactoryManagerTest extends TestCase {

  private File indexDir = new File("/tmp/static-ba");
  private GazelleIndexSegmentImpl indexSegment; 
  private DirectoryBasedFactoryManager directoryBasedFactoryManager;
  private MapBasedIndexFactory indexFactory;
  public void setUp() throws Exception {
    SingleNodeStarter.rmrf(indexDir);
    indexDir.mkdir();
     File file = new File(DirectoryBasedFactoryManagerTest.class.getClassLoader().getResource("ba-conf-avro/sensei.properties").toURI());
    PropertiesConfiguration conf = new PropertiesConfiguration();
    conf.setDelimiterParsingDisabled(true);
    conf.load(file);
    SenseiPluginRegistry pluginRegistry = SenseiPluginRegistry.build(conf);  
    pluginRegistry.start();
     directoryBasedFactoryManager = pluginRegistry.getBeanByFullPrefix("ba.index.factory", DirectoryBasedFactoryManager.class);
     indexDir = directoryBasedFactoryManager.getDirectory();
    directoryBasedFactoryManager.start();
     indexSegment = TestUtil.createIndexSegment();
     indexFactory = (MapBasedIndexFactory) directoryBasedFactoryManager.getZoieInstance(1, 0);
  }
 
  public void tearDown() throws Exception {
    directoryBasedFactoryManager.stop();
    SingleNodeStarter.rmrf(indexDir);   
  }
  
  public void test1Registering3SegmentsAndDeleteTwoAfter() throws Exception {
    File tempIndexDir = new File(indexDir, "tmp");
    tempIndexDir.mkdirs();
    File createCompressedSegment = TestUtil.createCompressedSegment("segment1", indexSegment, tempIndexDir);
    FileUtils.copyFileToDirectory(createCompressedSegment, indexDir);
    File avroFile = new File(getClass().getClassLoader().getResource("data/sample_data.avro").toURI());
    FileUtils.copyFileToDirectory(avroFile, indexDir);
    File index2Dir = new File(indexDir, "segment2");
    index2Dir.mkdir();
    SegmentPersistentManager.flushToDisk(AvroSegmentCreator.readFromAvroFile(avroFile), index2Dir);
    directoryBasedFactoryManager.scanForNewSegments();
    System.out.println("");
    new Wait(){
      public boolean until() {return indexFactory.getIndexReaders().size() == 3;};
    }; 
   new File(indexDir, "sample_data.avro").delete();
   directoryBasedFactoryManager.scanForNewSegments();
   new Wait(){
     public boolean until() {return indexFactory.getIndexReaders().size() == 2;};
   }; 
   new File(indexDir, createCompressedSegment.getName()).delete();
   directoryBasedFactoryManager.scanForNewSegments();
   new Wait(){
     public boolean until() {return indexFactory.getIndexReaders().size() == 1;};
   }; 
   FileUtils.deleteDirectory(index2Dir);
   directoryBasedFactoryManager.scanForNewSegments();
   new Wait(){
     public boolean until() {return indexFactory.getIndexReaders().size() == 0;};
   }; 
   index2Dir.mkdirs();
   SegmentPersistentManager.flushToDisk(AvroSegmentCreator.readFromAvroFile(avroFile), index2Dir);
   directoryBasedFactoryManager.scanForNewSegments();
   new Wait(){
     public boolean until() {return indexFactory.getIndexReaders().size() == 1;};
   }; 
  }
 
}
