package com.senseidb.ba;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.senseidb.ba.trevni.impl.TForwardIndex;
import com.senseidb.ba.trevni.impl.TReaderImpl;

public class TReaderImplTest {
  
  private String dataDir = "";
  private TReaderImpl impl;
  private String colNames [];
  @Before
  public void setUp() throws IOException, ClassNotFoundException {
    String baseDir = System.getProperty("user.dir");
    String path = baseDir + "/sensei-ba/src/test/resources/data/sample_data.avro";
    File indexDir = new File(baseDir + "/sensei-ba/src/test/resources/data/index");
    indexDir.mkdir();
    File avroFile = new File(path);
    DataMaker.createTrevniFilesFor(avroFile, indexDir.getAbsolutePath());
    File baseIndexDir = new File(indexDir.getAbsolutePath());
    impl = new TReaderImpl(baseIndexDir);
    Map<String, Class<?>> colTypes = impl.getColumnTypes();
    colNames = colTypes.keySet().toArray(new String[0]);
  }

  /*
   * Simple null check tests
   * */
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
    for (String colName : colNames) {
      if (!colName.startsWith("time_") && !colName.startsWith("met_"))
        assertNotNull(impl.getDictionary(colName));
    }
  }

  @Test
  public void testGetForwardIndex() throws Exception {
    Map<String, Class<?>> colTypes = impl.getColumnTypes();
    assertNotNull(colTypes);
    for (String colName : colNames) {
      assertNotNull(impl.getForwardIndex(colName));
    }
  }

  /*
   * End Simple null check tests
   * */

  @Test
  public void validateGetForwardIndexResponse() throws Exception {
    Map<String, Class<?>> colTypes = impl.getColumnTypes();
    assertNotNull(colTypes);
    for (String colName : colNames) {
      TForwardIndex idx = (TForwardIndex) impl.getForwardIndex(colName);
      assertNotNull(idx.getValueIndex(100));
    }
  }

}
