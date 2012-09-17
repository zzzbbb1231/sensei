package com.senseidb.ba.index1;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.senseidb.ba.IndexSegmentImpl;
import com.senseidb.ba.index1.InMemoryAvroMapper;
import com.senseidb.ba.index1.SegmentPersistentManager;
import com.senseidb.util.SingleNodeStarter;

public class SegmentPersistentManagerTest extends TestCase{

    private File avroFile;
    private File indexDir;

    @Before
    public void setUp() throws Exception {
        indexDir = new File("testIndex");
        SingleNodeStarter.rmrf(indexDir);
        indexDir.mkdir();
        indexDir = new File(indexDir, "segment");
        indexDir.mkdir();
        avroFile = new File(getClass().getClassLoader().getResource("data/sample_data.avro").toURI());

    }

    @After
    public void tearDown() throws Exception {
        SingleNodeStarter.rmrf(indexDir);

    }
@Test
    public void test1() throws Exception {
        FileInputStream avroFileStream = new FileInputStream(avroFile);
        InMemoryAvroMapper avroMapper = new InMemoryAvroMapper(avroFileStream);
        IndexSegmentImpl indexSegmentImpl = avroMapper.build();
        SegmentPersistentManager segmentPersistentManager = new SegmentPersistentManager();
        segmentPersistentManager.persist(indexDir, indexSegmentImpl);
        IndexSegmentImpl persistedIndexSegment = segmentPersistentManager.read(indexDir, false);
        System.out.println(FileUtils.readFileToString(segmentPersistentManager.getPropertiesMetadata(indexDir).getFile()));
        IOUtils.closeQuietly(avroFileStream);
    }


}
