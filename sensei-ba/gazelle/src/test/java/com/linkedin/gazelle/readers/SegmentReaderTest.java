package com.linkedin.gazelle.readers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
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
import com.linkedin.gazelle.dao.GazelleForwardIndexImpl;
import com.linkedin.gazelle.dao.GazelleIndexSegmentImpl;
import com.linkedin.gazelle.flushers.SegmentFlusher;
import com.linkedin.gazelle.utils.GazelleColumnMedata;
import com.linkedin.gazelle.utils.CompressedIntArray;
import com.linkedin.gazelle.utils.ReadMode;
import com.linkedin.gazelle.creators.SegmentCreator;

public class SegmentReaderTest {

  private static Logger logger = Logger.getLogger(SegmentReaderTest.class);
  private File _indexDir;
  private File _avroFile;
  private File _jsonFile;
  private GazelleIndexSegmentImpl _segment;
  
  @Before
  public void setup() throws IOException, ConfigurationException, URISyntaxException {
    _indexDir = new File("index");
    FileUtils.deleteDirectory(_indexDir);
    _indexDir.mkdir();
    _avroFile = new File(getClass().getClassLoader().getResource("data/sample_data.avro").toURI());
    _jsonFile = new File(getClass().getClassLoader().getResource("data/sample_data.json").toURI());
    SegmentCreator writer = new SegmentCreator();
    _segment = writer.process(_avroFile);
    SegmentFlusher.flush(_segment, _indexDir.getAbsolutePath());
  }

  @Test
  public void testmetadataDataAccess() throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, ReadMode.DBBuffer);
    HashMap<String, GazelleColumnMedata> metadataMap = segment.getColumnMetatdaMap();
    for (String column : metadataMap.keySet()) {
      assertNotNull(metadataMap.get(column).getName());
      assertNotNull(metadataMap.get(column).getBitsPerElement());
      assertNotNull(metadataMap.get(column).getByteLength());
      assertNotNull(metadataMap.get(column).getNumberOfDictionaryValues());
      assertNotNull(metadataMap.get(column).getNumberOfElements());
      assertNotNull(metadataMap.get(column).getColumnType());
      assertNotNull(metadataMap.get(column).getStartOffset());
    }
  }

  @Test
  public void testdictionaryDataAccess() throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, ReadMode.DBBuffer);
    HashMap<String, GazelleColumnMedata> metadataMap = segment.getColumnMetatdaMap();
    for (String column : metadataMap.keySet()) {
      switch (metadataMap.get(column).getColumnType()) {
        case FLOAT:
          TermFloatList floatList = (TermFloatList) segment.getDictionary(column);
          for (int i = 0; i < floatList.size(); i++) {
            assertNotNull(floatList.get(i));
          }
          break;
        case INT:
          TermIntList intList = (TermIntList) segment.getDictionary(column);
          for (int i=0; i < intList.size(); i++) {
            assertNotNull(intList.get(i));
          }
          break;
        case LONG:
          TermLongList longList = (TermLongList) segment.getDictionary(column);
          for (int i=0; i < longList.size(); i++) {
            assertNotNull(longList.get(i));
          }
          break;
        case STRING:
          TermStringList stringList = (TermStringList) segment.getDictionary(column);
          for (int i=0; i < stringList.size(); i++) {
            assertNotNull(stringList.get(i));
          }
          break;
        default:
          break;
      }
    }
  }

  @Test
  public void testgetLength() throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, ReadMode.DBBuffer);
    assertNotNull(segment.getLength());    
  }

  @Test
  public void testsegmentIndexDataValidityDBBufferMode() throws IOException, JSONException, ConfigurationException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, ReadMode.DBBuffer);
    HashMap<String, GazelleColumnMedata> metadataMap = segment.getColumnMetatdaMap();
    int i =1;
    for (String line : FileUtils.readLines(_jsonFile)) {
      JSONObject obj = new JSONObject(line);
      for (String column : metadataMap.keySet()) {
        GazelleForwardIndexImpl index = (GazelleForwardIndexImpl) segment.getForwardIndex(column);
        TermValueList<?> list = index.getDictionary();
        String valueFromJson = obj.get(column).toString();
        int forwardIndexValue = index.getValueIndex(i);
        String valueFromDict = list.get(forwardIndexValue).toString();
        valueFromDict = valueFromDict.replaceFirst("^0+(?!$)", "");
        valueFromDict = valueFromDict.replaceFirst("^-0+(?!$)", "-");
        assertEquals(valueFromJson,valueFromDict);
      }
     i++; 
    }
  }

  @Test
  public void testsegmentIndexDataValidityMMappedMode() throws IOException, JSONException, ConfigurationException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, ReadMode.MMAPPED);
    HashMap<String, GazelleColumnMedata> metadataMap = segment.getColumnMetatdaMap();
    int i =1;
    for (String line : FileUtils.readLines(_jsonFile)) {
      JSONObject obj = new JSONObject(line);
      for (String column : metadataMap.keySet()) {
        GazelleForwardIndexImpl index = (GazelleForwardIndexImpl) segment.getForwardIndex(column);
        TermValueList<?> list = index.getDictionary();
        String valueFromJson = obj.get(column).toString();
        int forwardIndexValue = index.getValueIndex(i);
        String valueFromDict = list.get(forwardIndexValue).toString();
        valueFromDict = valueFromDict.replaceFirst("^0+(?!$)", "");
        valueFromDict = valueFromDict.replaceFirst("^-0+(?!$)", "-");
        assertEquals(valueFromJson,valueFromDict);
      }
     i++; 
    }
  }

  @Test
  public void testrandomScanPerfForMMappedMode() throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, ReadMode.MMAPPED);
    HashMap<String, GazelleColumnMedata> metadataMap = segment.getColumnMetatdaMap();
    long start = System.currentTimeMillis();
    for (String column : metadataMap.keySet()) {
      GazelleForwardIndexImpl forwardIndex = (GazelleForwardIndexImpl) segment.getForwardIndex(column);
      int min = 0;
      int max = forwardIndex.getLength() - 1;
      for (int i = 0; i < forwardIndex.getLength(); i++) {
        int rand = min + (int)(Math.random() * ((max - min)));
        assertNotNull(forwardIndex.getValueIndex(rand));
      }
    }
    long stop = System.currentTimeMillis();
    logger.info("Time taken for a random scan on the entire Index in MMApped Mode is :" + (stop-start));
  }

  @Test
  public void testrandomScanPerfForDBBufferMode() throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, ReadMode.DBBuffer);
    HashMap<String, GazelleColumnMedata> metadataMap = segment.getColumnMetatdaMap();
    long start = System.currentTimeMillis();
    for (String column : metadataMap.keySet()) {
      GazelleForwardIndexImpl forwardIndex = (GazelleForwardIndexImpl) segment.getForwardIndex(column);
      int min = 0;
      int max = forwardIndex.getLength() - 1;
      for (int i = 0; i < forwardIndex.getLength(); i++) {
        int rand = min + (int)(Math.random() * ((max - min)));
        assertNotNull(forwardIndex.getValueIndex(rand));
      }
    }
    long stop = System.currentTimeMillis();
    logger.info("Time taken for a random scan on the entire Index in DBBUffer Mode is :" + (stop-start));
  }

  @Test
  public void testsequentialScanPerfForMMappedMode() throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, ReadMode.MMAPPED);
    HashMap<String, GazelleColumnMedata> metadataMap = segment.getColumnMetatdaMap();
    long start = System.currentTimeMillis();
    for (String column : metadataMap.keySet()) {
      GazelleForwardIndexImpl forwardIndex = (GazelleForwardIndexImpl) segment.getForwardIndex(column);
      for (int i = 0; i < forwardIndex.getLength(); i++) {
        assertNotNull(forwardIndex.getValueIndex(i));
      }
    }
    long stop = System.currentTimeMillis();
    logger.info("Time taken for a sequential scan on on the entire in MMApped Mode Index is :" + (stop-start));
  }

  @Test
  public void testsequentialScanPerfForDBBufferMode() throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, ReadMode.DBBuffer);
    HashMap<String, GazelleColumnMedata> metadataMap = segment.getColumnMetatdaMap();
    long start = System.currentTimeMillis();
    for (String column : metadataMap.keySet()) {
      GazelleForwardIndexImpl forwardIndex = (GazelleForwardIndexImpl) segment.getForwardIndex(column);
      for (int i = 0; i < forwardIndex.getLength(); i++) {
        assertNotNull(forwardIndex.getValueIndex(i));
      }
    }
    long stop = System.currentTimeMillis();
    logger.info("Time taken for a sequential scan on the entire Index in DBBuffer Mode is :" + (stop-start));
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(_indexDir);
  }
}
