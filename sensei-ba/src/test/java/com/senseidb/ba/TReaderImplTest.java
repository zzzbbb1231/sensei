package com.senseidb.ba;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class TReaderImplTest {
  
  private String dataDir = "";
  private TReaderImpl impl;
  private String colNames [];
  @Before
  public void setUp() throws IOException, ClassNotFoundException {
    String dir = System.getProperty("user.dir");
    String path = dir + "/sensei-ba/src/testdata/sample_data.avro";
    File dir1 = new File(dir + "/src/data/index");
    dataDir = dir + "/sensei-ba/src/testdata/index";
    dir1.mkdir();
    File avroFile = new File(path);
    DataMaker.createTrevniFilesFor(avroFile, dir1.getAbsolutePath());
    File baseDir = new File(dataDir);
    impl = new TReaderImpl(baseDir);
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
    /*
     * Every non-metric and non-time column should have a dictionary 
     * */
    for (String colName : colNames) {
      assertNotNull(impl.getForwardIndex(colName));
    }
  }

  /*
   * End Simple null check tests
   * */

}
