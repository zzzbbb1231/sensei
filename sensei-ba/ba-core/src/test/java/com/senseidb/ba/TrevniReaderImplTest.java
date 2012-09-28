package com.senseidb.ba;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.senseidb.ba.trevni.impl.TrevniReaderImpl;
import com.senseidb.util.SingleNodeStarter;

public class TrevniReaderImplTest {
  
  private TrevniReaderImpl impl;
  private String colNames [];
  
  /*
   * Preserving the schema of the original avro file from which the trv files are created,
   * to run validation tests if needed. 
   * 
   * */
  private Schema schema;
  
  @Before
  public void setUp() throws Exception {
    File indexDir = new File("index"); 
    SingleNodeStarter.rmrf(indexDir);
    indexDir.mkdir();
    File avroFile = new File(getClass().getClassLoader().getResource("data/sample_data.avro").toURI());
    schema = null;
    File baseIndexDir = new File(indexDir.getAbsolutePath());
    impl = new TrevniReaderImpl(baseIndexDir);
    Map<String, Class<?>> colTypes = impl.getColumnTypes();
    colNames = colTypes.keySet().toArray(new String[0]);
  }
  @After
  public void tearDown() throws Exception {
    File indexDir = new File("index"); 
    SingleNodeStarter.rmrf(indexDir);
  
  }
  /*
   * Simple null check tests
   * */
  
  @Test
  public void testGetLength() throws Exception {
    assertNotNull(impl.getLength());
    assertNotSame(0, impl.getLength());
  }

  @Test
  public void testGetColumnTypes() throws Exception {
    Map<String, Class<?>> colTypes = impl.getColumnTypes();
    assertNotNull(colTypes);
    for (String colName : colTypes.keySet()) {
      assertNotNull(colTypes.get(colName));
    }
  }

  @Test
  public void testGetDictionary() throws Exception {
    Map<String, Class<?>> colTypes = impl.getColumnTypes();
    assertNotNull(colTypes);
    /*
     * Every non-metric and non-time column should have a dictionary 
     * */
    for (String colName : colNames) 
      
        assertNotNull(impl.getDictionary(colName));
   
  }

  @Test
  public void testGetForwardIndex() throws Exception {
    Map<String, Class<?>> colTypes = impl.getColumnTypes();
    assertNotNull(colTypes);
    for (String colName : colNames) {
      //assertNotNull(impl.getForwardIndex(colName));
    }
  }
}
