package com.senseidb.ba.index1;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonGenerator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.SortedForwardIndex;
import com.senseidb.ba.gazelle.creators.SegmentCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.impl.MultiValueForwardIndexImpl1;
import com.senseidb.ba.gazelle.impl.SortedForwardIndexImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.gazelle.utils.ReadMode;
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
    public void test1LoadPersistReadAndCompareWithJson() throws Exception {
        try {
       FileInputStream avroFileStream = new FileInputStream(avroFile);
        /*dumpToJson(avroFileStream, new PrintStream( new FileOutputStream(new File("json.txt"))));
        avroFileStream = new FileInputStream(avroFile);*/
        GazelleIndexSegmentImpl indexSegmentImpl = SegmentCreator.readFromAvroFile(avroFile);
        MultiValueForwardIndexImpl1 forwardIndexImpl1 = (MultiValueForwardIndexImpl1) indexSegmentImpl.getForwardIndex("dim_skills");        
        SegmentPersistentManager.flushToDisk(indexSegmentImpl, indexDir);
        GazelleIndexSegmentImpl persistedIndexSegment = SegmentPersistentManager.read(indexDir, ReadMode.DirectMemory);
        
        IOUtils.closeQuietly(avroFileStream);
        compareWithJsonFile(indexSegmentImpl);
        compareWithJsonFile(persistedIndexSegment);
        } catch (Throwable throwable) {
          throwable.printStackTrace();
        }
    }
@Test
public void test2CheckForwardIndexes() throws Exception {
    FileInputStream avroFileStream = new FileInputStream(avroFile);
    /*dumpToJson(avroFileStream, new PrintStream( new FileOutputStream(new File("json.txt"))));
    avroFileStream = new FileInputStream(avroFile);*/
    GazelleIndexSegmentImpl indexSegmentImpl = SegmentCreator.readFromAvroFile(avroFile);
   
    SegmentPersistentManager.flushToDisk(indexSegmentImpl, indexDir);
    GazelleIndexSegmentImpl persistedIndexSegment = SegmentPersistentManager.read(indexDir, ReadMode.DirectMemory);
    ForwardIndex forwardIndex = persistedIndexSegment.getForwardIndex("shrd_advertiserId");
    assertTrue(forwardIndex instanceof SortedForwardIndexImpl);
    assertEquals(Arrays.toString(new int[] {-1, 0 , 1, 3, 6}),Arrays.toString( ((SortedForwardIndex) forwardIndex).getMinDocIds()));
    assertEquals(Arrays.toString(new int[] {-1, 0 ,2, 5, 9999}), Arrays.toString(((SortedForwardIndex) forwardIndex).getMaxDocIds()));
    
}
private void compareWithJsonFile(GazelleIndexSegmentImpl indexSegmentImpl) throws URISyntaxException, IOException, JSONException {
    File jsonFile = new File(getClass().getClassLoader().getResource("data/sample_data.json").toURI());
    int i = 0;
    for (String line : FileUtils.readLines(jsonFile)) {
        JSONObject json = new JSONObject(line);
       
        for (String column : indexSegmentImpl.getColumnTypes().keySet()) {
            if (indexSegmentImpl.getForwardIndex(column) instanceof MultiValueForwardIndex) {
                MultiValueForwardIndex multiValueForwardIndex = (MultiValueForwardIndex) indexSegmentImpl.getForwardIndex(column);
                JSONArray object = json.optJSONArray(column);
                int[] buffer = new int[multiValueForwardIndex.getMaxNumValuesPerDoc()];
                int count = multiValueForwardIndex.randomRead(buffer, i);                
                if (object == null) {
                    assertEquals(0, count);
                } else {
                    for (int j = 0; j < object.length(); j++) {
                        count--;
                        assertEquals(prepareValue(object.getString(j)), prepareValue(multiValueForwardIndex.getDictionary().get(buffer[j])));
                    }
                }
                continue;
            }
            SingleValueForwardIndex forwardIndex = (SingleValueForwardIndex) indexSegmentImpl.getForwardIndex(column);
            TermValueList<?> dictionary = indexSegmentImpl.getDictionary(column);
            
                String jsonValue = json.get(column).toString();
                int valueIndex = forwardIndex.getValueIndex(i);
               
                String value = dictionary.get(valueIndex);
                value = prepareValue(value);
                assertEquals(jsonValue, value);
        }
        i++;
    }
}

private static String prepareValue(String value) {
    value = value.replaceFirst("^0+(?!$)", "");
    value = value.replaceFirst("^-0+(?!$)", "-");
    return value;
}
public void dumpToJson(InputStream fileStream, PrintStream out) throws Exception {
     

      GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
      DataFileStream fileReader =
        new DataFileStream(fileStream, reader);
      try {
        Schema schema = fileReader.getSchema();
        DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
        Encoder encoder = new JsonEncoder(schema, (JsonGenerator)null);
        for (Object datum : fileReader) {
          // init() recreates the internal Jackson JsonGenerator
          encoder.init(out);
          writer.write(datum, encoder);
          encoder.flush();
          out.println();
        }
        out.flush();
      } finally {
        fileReader.close();
      }
      //return 0;
    }

}
