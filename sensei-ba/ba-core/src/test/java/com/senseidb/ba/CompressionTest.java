package com.senseidb.ba;

import java.io.File;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.senseidb.ba.gazelle.creators.AvroSegmentCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.util.TarGzCompressionUtils;
import com.senseidb.util.SingleNodeStarter;

public class CompressionTest extends TestCase {
  private File avroFile;
  private File indexDir;
  private File segmentDir;

  @Before
  public void setUp() throws Exception {
      indexDir = new File("testIndex");
      SingleNodeStarter.rmrf(indexDir);
      indexDir.mkdir();
      segmentDir = new File(indexDir, "segment1");
      indexDir.mkdir();
      avroFile = new File(getClass().getClassLoader().getResource("data/sample_data.avro").toURI());
      GazelleIndexSegmentImpl indexSegmentImpl = AvroSegmentCreator.readFromAvroFile(avroFile);
      SegmentPersistentManager.flushToDisk(indexSegmentImpl, segmentDir);

  }
  @After
  public void tearDown() throws Exception {
      SingleNodeStarter.rmrf(indexDir);

  }
  public void test1() throws Exception {
    File compressedFile = new File(indexDir, "compressedIndex.tar.gz");
    TarGzCompressionUtils.createTarGzOfDirectory(segmentDir.getAbsolutePath() + "/", compressedFile.getAbsolutePath());
    SingleNodeStarter.rmrf(segmentDir);
    TarGzCompressionUtils.unTar(compressedFile, indexDir);
    
    GazelleIndexSegmentImpl read = SegmentPersistentManager.read(segmentDir, ReadMode.DirectMemory);
    assertNotNull(read);
  }
}
