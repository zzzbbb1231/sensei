package com.senseidb.ba.gazelle.readers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

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
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.creators.AvroSegmentCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.ReadMode;



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
    System.out.println(_indexDir.getAbsolutePath());
    _segment = AvroSegmentCreator.readFromAvroFile(_avroFile);
    SegmentPersistentManager.flushToDisk(_segment, _indexDir);
  }


  @Test
  public void testdictionaryDataAccess() throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentPersistentManager.read(_indexDir, ReadMode.DirectMemory);
    Map<String, ColumnMetadata> metadataMap = segment.getColumnMetadataMap();
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
    GazelleIndexSegmentImpl segment = SegmentPersistentManager.read(_indexDir, ReadMode.DirectMemory);
    assertTrue(segment.getLength() > 0);
  }

  @Test

  public void testsegmentIndexDataValidityDBBufferModeNew() throws ConfigurationException, IOException, JSONException {
    testsegmentIndexDataValidityFor(ReadMode.DirectMemory);
  }

  @Test
  public void testsegmentIndexDataValidityMMappedModeNew() throws ConfigurationException, IOException, JSONException {
    testsegmentIndexDataValidityFor(ReadMode.MemoryMapped);
  }

  public void testsegmentIndexDataValidityFor(ReadMode mode) throws IOException, JSONException, ConfigurationException {
    GazelleIndexSegmentImpl segment = SegmentPersistentManager.read(_indexDir, mode);
    Map<String, ColumnMetadata> metadataMap = segment.getColumnMetadataMap();

    int i = 1;
    for (String line : FileUtils.readLines(_jsonFile)) {

      JSONObject obj = new JSONObject(line);
      for (String column : metadataMap.keySet()) {
          SingleValueForwardIndex index =  (SingleValueForwardIndex) segment.getForwardIndex(column);
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
    testrandomScanPerfFor(ReadMode.MemoryMapped);
  }

  @Test
  public void testRandomScanPerfForDBBufferMode() throws ConfigurationException, IOException {
    testrandomScanPerfFor(ReadMode.DirectMemory);
  }

  public void testrandomScanPerfFor(ReadMode mode) throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentPersistentManager.read(_indexDir, mode);
    Map<String, ColumnMetadata> metadataMap = segment.getColumnMetadataMap();
    long start = System.currentTimeMillis();
    for (String column : metadataMap.keySet()) {
        SingleValueForwardIndex forwardIndex =  (SingleValueForwardIndex) segment.getForwardIndex(column);
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
    testSequentialScanPerfFor(ReadMode.MemoryMapped);
  }

  @Test
  public void testSequentialScanPerfForDBBufferMode() throws ConfigurationException, IOException {
    testSequentialScanPerfFor(ReadMode.DirectMemory);
  }

  public void testSequentialScanPerfFor(ReadMode mode) throws ConfigurationException, IOException {
    GazelleIndexSegmentImpl segment = SegmentPersistentManager.read(_indexDir, mode);
    Map<String, ColumnMetadata> metadataMap = segment.getColumnMetadataMap();
    long start = System.currentTimeMillis();
    for (String column : metadataMap.keySet()) {
        SingleValueForwardIndex forwardIndex =  (SingleValueForwardIndex) segment.getForwardIndex(column);
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
    //FileUtils.deleteDirectory(_indexDir);
  }
}
