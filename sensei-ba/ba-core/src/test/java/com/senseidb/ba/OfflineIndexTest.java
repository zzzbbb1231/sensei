package com.senseidb.ba;

import java.io.File;

import junit.framework.TestCase;

import org.junit.Test;

import com.senseidb.ba.format.JsonDataSource;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;

public class OfflineIndexTest extends TestCase {

  @Test
  public void test1() throws Exception {
  
    IndexSegment offlineSegment =  com.senseidb.ba.format.GenericIndexCreator.create(new JsonDataSource(new File(OfflineIndexTest.class.getClassLoader().getResource("data/test_data.json").toURI())));
    assertEquals(15000, offlineSegment.getLength());
  }
  @Test
  public void test2() throws Exception {
  
    IndexSegment offlineSegment =  com.senseidb.ba.format.GenericIndexCreator.create(new File(OfflineIndexTest.class.getClassLoader().getResource("data/sample_data.csv").toURI()));
    assertEquals(10000, offlineSegment.getLength());
    System.out.println(((GazelleIndexSegmentImpl)offlineSegment).getColumnMetadataMap());
  }
}
