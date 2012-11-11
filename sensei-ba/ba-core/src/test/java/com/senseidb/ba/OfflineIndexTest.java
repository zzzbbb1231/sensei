package com.senseidb.ba;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.junit.Test;

import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.creators.GenericIndexCreator;

public class OfflineIndexTest extends TestCase {

  @Test
  public void test1() throws Exception {
  
    IndexSegment offlineSegment =  GenericIndexCreator.create(new JsonDataSource(new File(OfflineIndexTest.class.getClassLoader().getResource("data/test_data.json").toURI())));
    assertEquals(15000, offlineSegment.getLength());
  }

}
