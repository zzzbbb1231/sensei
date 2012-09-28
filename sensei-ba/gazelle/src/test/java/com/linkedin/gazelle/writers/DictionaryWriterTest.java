package com.linkedin.gazelle.writers;

/**
 @author dpatel
 */

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.gazelle.TestHelper;
import com.linkedin.gazelle.utils.ColumnMedata;

public class DictionaryWriterTest {

  private DictionaryWriter _dictionaryWriter;
  private File _indexDir;
  private Schema _avroSchema;
  private ColumnMedata[] _columnMetaMedataArr;
  DataFileStream<GenericRecord> _dataFileReader;

  @Before
  public void setup() throws IOException {
    _indexDir = new File("index");
    _indexDir.delete();
    _indexDir.mkdir();

    String avroFilepath = System.getProperty("user.dir")
        + "/src/test/resources/data/sample_data.avro";
    File avroFile = new File(avroFilepath);
    InputStream is = new FileInputStream(avroFile);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    _dataFileReader = new DataFileStream<GenericRecord>(is, datumReader);
    _avroSchema = _dataFileReader.getSchema();
    _columnMetaMedataArr = TestHelper.setUpColumnMetadataArr(_avroSchema);
  }

  @Test
  public void validityCheck() {
    _dictionaryWriter = new DictionaryWriter(_columnMetaMedataArr[8]);
    int count = 1;
    assertNotNull(_dictionaryWriter);
    while (_dataFileReader.hasNext()) {
      GenericRecord record = _dataFileReader.next();
      Object columnEntry = record.get(_columnMetaMedataArr[8].getName());
      if (columnEntry instanceof Utf8) {
        columnEntry = ((Utf8) columnEntry).toString();
      }
      _dictionaryWriter.addValue(columnEntry);
    }
    _dictionaryWriter.getTermValueList();
  }

  @After
  public void tearDown() {
    _indexDir.delete();
  }
}
