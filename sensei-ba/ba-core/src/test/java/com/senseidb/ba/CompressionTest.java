package com.senseidb.ba;

import java.io.File;

import org.junit.After;
import org.junit.Before;

import com.senseidb.ba.index1.InMemoryAvroMapper;
import com.senseidb.ba.index1.SegmentPersistentManager;
import com.senseidb.ba.util.TarGzCompressionUtils;
import com.senseidb.util.SingleNodeStarter;

import junit.framework.TestCase;

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
      IndexSegmentImpl indexSegmentImpl = new InMemoryAvroMapper(avroFile).build();
      SegmentPersistentManager.persist(segmentDir, indexSegmentImpl);

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
    
    IndexSegmentImpl read = SegmentPersistentManager.read(segmentDir, false);
    assertNotNull(read);
  }
}
