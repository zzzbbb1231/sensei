package com.linkedin.gazelle.readers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
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
import com.linkedin.gazelle.creators.SegmentCreator;
import com.linkedin.gazelle.dao.GazelleForwardIndexImpl;
import com.linkedin.gazelle.dao.GazelleIndexSegmentImpl;
import com.linkedin.gazelle.flushers.SegmentFlusher;
import com.linkedin.gazelle.utils.CompressedIntArray;
import com.linkedin.gazelle.utils.GazelleColumnMedata;
import com.linkedin.gazelle.utils.ReadMode;

public class SegmentReaderTest {

  private static Logger logger = Logger.getLogger(SegmentReaderTest.class);
  private File _indexDir;
  private File _avroFile;
  private String _jsonFilePath;
  private ReadMode _mode;
  private GazelleIndexSegmentImpl _segment;

  @Before
  public void setup() throws IOException, ConfigurationException, URISyntaxException {
    _indexDir = new File("index");
    FileUtils.deleteDirectory(_indexDir);
    _indexDir.mkdir();
    String avroFilepath =
        System.getProperty("user.dir") + "/sensei-ba/gazelle/src/test/resources/data/sample_data.avro";
    _jsonFilePath = System.getProperty("user.dir") + "/sensei-ba/gazelle/src/test/resources/data/sample_data.json";
    _avroFile = new File(avroFilepath);
    SegmentCreator writer = new SegmentCreator();
    _segment = writer.process(_avroFile);
    SegmentFlusher.flush(_segment, _indexDir.getAbsolutePath());
  }

  @Test
  public void testmetadataDataAccess() throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, ReadMode.DBBuffer);
    HashMap<String, GazelleColumnMedata> metadataMap = segment.getColumnMetatdaMap();
    HashMap<String, CompressedIntArray> compressedIntArrayMap = segment.getCompressedIntArrayMap();
    HashMap<String, TermValueList> termValueListMap = segment.getTermValueListMap();

    for (String column : metadataMap.keySet()) {
      assertFalse(StringUtils.isBlank(metadataMap.get(column).getName()));
      int numOfBits = CompressedIntArray.getNumOfBits(termValueListMap.get(column).size());
      assertEquals(numOfBits, (metadataMap.get(column).getBitsPerElement() != 0));
      assertTrue((metadataMap.get(column).getByteLength() != 0));
      assertEquals(termValueListMap.size(), metadataMap.get(column).getNumberOfDictionaryValues());
      assertEquals(compressedIntArrayMap.get(column).getCapacity(), metadataMap.get(column).getNumberOfElements() != 0);
      assertTrue((StringUtils.isBlank(metadataMap.get(column).getColumnType().toString())));
      assertEquals(compressedIntArrayMap.get(column).getCapacity(), metadataMap.get(column).getNumberOfElements());
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
          assertTrue(floatList.get(i) != null);
        }
        break;
      case INT:
        TermIntList intList = (TermIntList) segment.getDictionary(column);
        for (int i = 0; i < intList.size(); i++) {
          assertTrue(intList.get(i) != null);
        }
        break;
      case LONG:
        TermLongList longList = (TermLongList) segment.getDictionary(column);
        for (int i = 0; i < longList.size(); i++) {
          assertTrue(longList.get(i) != null);
        }
        break;
      case STRING:
        TermStringList stringList = (TermStringList) segment.getDictionary(column);
        for (int i = 0; i < stringList.size(); i++) {
          assertTrue(stringList.get(i) != null);
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
    assertTrue(segment.getLength() > 0);
  }

  @Test
  public void testsegmentIndexDataValidityDBBufferModeNew() throws ConfigurationException, IOException, JSONException {
    testsegmentIndexDataValidityFor(ReadMode.DBBuffer);
  }

  @Test
  public void testsegmentIndexDataValidityMMappedModeNew() throws ConfigurationException, IOException, JSONException {
    testsegmentIndexDataValidityFor(ReadMode.MMAPPED);
  }

  public void testsegmentIndexDataValidityFor(ReadMode mode) throws IOException, JSONException, ConfigurationException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, mode);
    HashMap<String, GazelleColumnMedata> metadataMap = segment.getColumnMetatdaMap();
    int i = 1;
    File jsonFile = new File(_jsonFilePath);
    for (String line : FileUtils.readLines(jsonFile)) {
      JSONObject obj = new JSONObject(line);
      for (String column : metadataMap.keySet()) {
        GazelleForwardIndexImpl index = (GazelleForwardIndexImpl) segment.getForwardIndex(column);
        TermValueList<?> list = index.getDictionary();
        String valueFromJson = obj.get(column).toString();
        int forwardIndexValue = index.getValueIndex(i);
        String valueFromDict = list.get(forwardIndexValue).toString();
        valueFromDict = valueFromDict.replaceFirst("^0+(?!$)", "");
        valueFromDict = valueFromDict.replaceFirst("^-0+(?!$)", "-");
        assertEquals(valueFromJson, valueFromDict);
      }
      i++;
    }
  }

  @Test
  public void testRandomScanPerfForMMappedMode() throws ConfigurationException, IOException {
    testrandomScanPerfFor(ReadMode.MMAPPED);
  }

  @Test
  public void testRandomScanPerfForDBBufferMode() throws ConfigurationException, IOException {
    testrandomScanPerfFor(ReadMode.DBBuffer);
  }

  public void testrandomScanPerfFor(ReadMode mode) throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, mode);
    HashMap<String, GazelleColumnMedata> metadataMap = segment.getColumnMetatdaMap();
    long start = System.currentTimeMillis();
    for (String column : metadataMap.keySet()) {
      GazelleForwardIndexImpl forwardIndex = (GazelleForwardIndexImpl) segment.getForwardIndex(column);
      int min = 0;
      int max = forwardIndex.getLength() - 1;
      for (int i = 0; i < forwardIndex.getLength(); i++) {
        int rand = min + (int) (Math.random() * ((max - min)));
        assertTrue(forwardIndex.getValueIndex(rand) >= 0);
      }
    }
    long stop = System.currentTimeMillis();
    logger.info("Time taken for a random scan on the entire Index in " + mode.toString() + " Mode is :"
        + (stop - start));
  }

  @Test
  public void testSequentialScanPerfForMMappedMode() throws ConfigurationException, IOException {
    testSequentialScanPerfFor(ReadMode.MMAPPED);
  }

  @Test
  public void testSequentialScanPerfForDBBufferMode() throws ConfigurationException, IOException {
    testSequentialScanPerfFor(ReadMode.DBBuffer);
  }

  public void testSequentialScanPerfFor(ReadMode mode) throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentReader.read(_indexDir, mode);
    HashMap<String, GazelleColumnMedata> metadataMap = segment.getColumnMetatdaMap();
    long start = System.currentTimeMillis();
    for (String column : metadataMap.keySet()) {
      GazelleForwardIndexImpl forwardIndex = (GazelleForwardIndexImpl) segment.getForwardIndex(column);
      for (int i = 0; i < forwardIndex.getLength(); i++) {
        assertTrue(forwardIndex.getValueIndex(i) >= 0);
      }
    }
    long stop = System.currentTimeMillis();
    logger.info("Time taken for a sequential scan on on the entire Index in " + mode.toString() + " Mode Index is :"
        + (stop - start));
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(_indexDir);
  }
}
