package com.senseidb.ba.gazelle.readers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Test;

public class TempTest {

  private String inputFileDir = "/home/dpatel/beta/ads-sample-data";
  private String[] originalAvroFile =
      { "part-1.avro", "part-2.avro", "part-3.avro" };
  private String outputFileDir =
      "/home/dpatel/beta/ads-sample-data/ads-split-deta";
  private int count = 0;
  private int fileCounter = 1;
  private Schema schema;


  public void readAvro() throws IOException {
    File avroFile = new File(inputFileDir, originalAvroFile[0]);
    DatumReader<GenericRecord> datumReader =
        new GenericDatumReader<GenericRecord>();
    InputStream is = new FileInputStream(avroFile);
    DataFileStream<GenericRecord> dataFileReader =
        new DataFileStream<GenericRecord>(is, datumReader);
    schema = dataFileReader.getSchema();
    dataFileReader.close();
    is.close();
    DataFileWriter<GenericRecord> dataFileWriter = getNewWriter();
    
    for (String fileName : originalAvroFile) {
      File afile = new File(inputFileDir, fileName);
      DatumReader<GenericRecord> datumReadero =
          new GenericDatumReader<GenericRecord>();
      InputStream is1 = new FileInputStream(afile);
      DataFileStream<GenericRecord> dataFileReadero =
          new DataFileStream<GenericRecord>(is1, datumReadero);
      while (dataFileReadero.hasNext()) {
        if (count <= 10000000) {
          dataFileWriter.append(dataFileReadero.next());
        } else {
          fileCounter++;
          count = 0;
          dataFileWriter = getNewWriter();
        }
        count++;
      }
      dataFileReadero.close();
      is1.close();
    }
  }

  public DataFileWriter<GenericRecord> getNewWriter() throws IOException {
    File newFile = new File(outputFileDir, "part-" + fileCounter + ".avro");
    DatumWriter<GenericRecord> writer =
        new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
        new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, newFile);
    return dataFileWriter;
  }
  
  @Test 
  public void testCount() throws IOException {
    String path = "/home/dpatel/beta/ads-sample-data/ads-split-deta/part-1.avro";
    DatumReader<GenericRecord> datumReader =
        new GenericDatumReader<GenericRecord>();
    InputStream is = new FileInputStream(new File(path));
    DataFileStream<GenericRecord> dataFileReader =
        new DataFileStream<GenericRecord>(is, datumReader);
    schema = dataFileReader.getSchema();
    int items = schema.getFields().size();
    for (Field field : schema.getFields()) {
      System.out.println(field.name());
    }
    int c = 0;
    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();
      if (c < 10) {
        for (int j = 0; j < items; j++) {
          Object value = record.get(j);
          System.out.println(value.toString());
        }
      } 
      c++;
    }
    System.out.println(c);
    dataFileReader.close();
    is.close();
  }
}
