package com.linkedin.gazelle.readers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.utils.ColumnMedata;
import com.linkedin.gazelle.utils.CompressedIntArray;
import com.linkedin.gazelle.utils.ReadMode;
import com.linkedin.gazelle.writers.SegmentWriter;

public class SegmentReaderTest {

  private File _indexDir;
  private File _avroFile;
  private String _jsonFilePath;
  private ReadMode _mode;
  
  @Before
  public void setup() throws IOException {
    _indexDir = new File("index");
    _indexDir.delete();
    _indexDir.mkdir();
    String avroFilepath = System.getProperty("user.dir")
        + "/sensei-ba/gazelle/src/test/resources/data/sample_data.avro";
    _jsonFilePath = System.getProperty("user.dir") + "/sensei-ba/gazelle/src/test/resources/data/sample_data.json";
    _avroFile = new File(avroFilepath);
    SegmentWriter writer = new SegmentWriter(_avroFile);
    writer.process();
    writer.flushTo(_indexDir);
  }
  
  @Test
  public void testMMappedReadMode() throws ConfigurationException, IOException, JSONException {
    _mode = ReadMode.MMAPPED;
    validate();
    metadataDataAccessTest();
    dictionaryDataAccessTest();
    forwardIndexDataAccessTest();
    segmentIndexDataValidityTest();
  }
  
  @Test
  public void testDBBufferReadMode() throws ConfigurationException, IOException, JSONException {
    _mode = ReadMode.DBBuffer;
    validate();
    metadataDataAccessTest();
    dictionaryDataAccessTest();
    forwardIndexDataAccessTest();
    segmentIndexDataValidityTest();
  }

  private void validate() throws ConfigurationException, IOException {
    SegmentReader reader = new SegmentReader(_indexDir, _mode);
    assertNotNull(reader);
    assertNotNull(reader.readColumnMetadataMap());
    assertNotNull(reader.readDictionaryMap());
    assertNotNull(reader.readForwardIndexMap());
    HashMap<String, ColumnMedata> metadataMap = reader.readColumnMetadataMap();
    for (String column : metadataMap.keySet()) {
      assertNotNull(metadataMap.get(column));
    }
    
    HashMap<String, CompressedIntArray> compressedIntMap = reader.readForwardIndexMap();
    for (String column: compressedIntMap.keySet()) {
      assertNotNull(compressedIntMap.get(column));
    }
    
    HashMap<String, TermValueList> termValueListmap = reader.readDictionaryMap();
    for (String column : termValueListmap.keySet()) {
      assertNotNull(termValueListmap.get(column));
    }
  }

  private void metadataDataAccessTest() throws ConfigurationException {
    SegmentReader reader = new SegmentReader(_indexDir, _mode);
    HashMap<String, ColumnMedata> metadataMap = reader.readColumnMetadataMap();
    for (String column : metadataMap.keySet()) {
      assertNotNull(metadataMap.get(column).getName());
      assertNotNull(metadataMap.get(column).getBitsPerElement());
      assertNotNull(metadataMap.get(column).getByteLength());
      assertNotNull(metadataMap.get(column).getNumberOfDictionaryValues());
      assertNotNull(metadataMap.get(column).getNumberOfElements());
      assertNotNull(metadataMap.get(column).getOriginalType());
      assertNotNull(metadataMap.get(column).getStartOffset());
    }
  }

  private void dictionaryDataAccessTest() throws ConfigurationException {
    SegmentReader reader = new SegmentReader(_indexDir, _mode);
    HashMap<String, TermValueList> termValueListMap = reader.readDictionaryMap();
    HashMap<String, ColumnMedata> metadataMap = reader.readColumnMetadataMap();
    for (String column : termValueListMap.keySet()) {
      switch (metadataMap.get(column).getOriginalType()) {
        case FLOAT:
          TermFloatList floatList = (TermFloatList) termValueListMap.get(column);
          for (int i = 0; i < floatList.size(); i++) {
            assertNotNull(floatList.get(i));
          }
          break;
        case INT:
          TermIntList intList = (TermIntList) termValueListMap.get(column);
          for (int i=0; i < intList.size(); i++) {
            assertNotNull(intList.get(i));
          }
          break;
        case LONG:
          TermLongList longList = (TermLongList) termValueListMap.get(column);
          for (int i=0; i < longList.size(); i++) {
            assertNotNull(longList.get(i));
          }
          break;
        case STRING:
          TermStringList stringList = (TermStringList) termValueListMap.get(column);
          for (int i=0; i < stringList.size(); i++) {
            assertNotNull(stringList.get(i));
          }
          break;
        default:
          break;
      }
    }
  }

  private void forwardIndexDataAccessTest() throws ConfigurationException, IOException {
    SegmentReader reader = new SegmentReader(_indexDir, _mode);
    HashMap<String, ColumnMedata> metadataMap = reader.readColumnMetadataMap();
    HashMap<String, CompressedIntArray> compressedIntArrMap = reader.readForwardIndexMap();
    
    for (String column : compressedIntArrMap.keySet()) {
      for (int i = 0; i < compressedIntArrMap.get(column).getCapacity(); i++) {
        assertNotNull(compressedIntArrMap.get(column).readInt(i));
      }
    }
  }

  private void segmentIndexDataValidityTest() throws IOException, JSONException, ConfigurationException {
    SegmentReader reader = new SegmentReader(_indexDir, _mode);
    HashMap<String, CompressedIntArray> compressedIntArrMap = reader.readForwardIndexMap();
    HashMap<String, TermValueList> termValueListMap = reader.readDictionaryMap();
    int i =1;
    File jsonFile = new File(_jsonFilePath);
    for (String line : FileUtils.readLines(jsonFile)) {
      JSONObject obj = new JSONObject(line);
      for (String column : compressedIntArrMap.keySet()) {
        CompressedIntArray arr = compressedIntArrMap.get(column);
        TermValueList<?> list = termValueListMap.get(column);
        String valueFromJson = obj.get(column).toString();
        int forwardIndexValue = arr.readInt(i);
        String valueFromDict = list.get(forwardIndexValue).toString();
        valueFromDict = valueFromDict.replaceFirst("^0+(?!$)", "");
        valueFromDict = valueFromDict.replaceFirst("^-0+(?!$)", "-");
        assertEquals(valueFromJson,valueFromDict);
      }
     i++; 
    }
  }

  @After
  public void tearDown() {
  }
}
