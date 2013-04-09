package com.senseidb.ba;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

public class AvroConverter {
public static void main(String[] args) throws Exception {
 /* File jsonFile = new File(AvroConverter.class.getClassLoader().getResource("data/sample_data.json").toURI());
  File avroSchema = new File(AvroConverter.class.getClassLoader().getResource("data/avro.schema").toURI());
  jsonToAvro(jsonFile, avroSchema, "src/test/resources/data/sample_data.avro");*/
  dumpAvro();
}
public static void dumpAvro() throws URISyntaxException, FileNotFoundException, IOException {
  File avroFile = new File("/home/vzhabiuk/work/tmp/tesla/tesla.avro");
  DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
  FileInputStream inputStream1 = new FileInputStream(avroFile);
    DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(inputStream1, datumReader);

  Schema schema = dataFileReader.getSchema();
  int i = 0;
  while (dataFileReader.hasNext() && i < 2) {
    System.out.println(dataFileReader.next());
    i++;
  }
  //FileUtils.writeStringToFile(new File("avro.schema"), schema.toString(true));
}
public static void createSmallerAvro() throws URISyntaxException, FileNotFoundException, IOException {
  File avroFile = new File("/home/vzhabiuk/work/tmp/tesla/tesla.avro");
  DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
  FileInputStream inputStream1 = new FileInputStream(avroFile);
    DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(inputStream1, datumReader);

  Schema schema = dataFileReader.getSchema();
  int i = 0;
  DatumWriter<GenericRecord> writer=new GenericDatumWriter<GenericRecord>(schema);
  DataFileWriter<GenericRecord> dataFileWriter=new DataFileWriter<GenericRecord>(writer);

  
  dataFileWriter.create(schema, new File("/home/vzhabiuk/work/tmp/tesla/manyMetrics.avro"));
  while (dataFileReader.hasNext() && i < 1000) {
    dataFileWriter.append(dataFileReader.next());
    i++;
  }
  dataFileWriter.close();
  FileUtils.writeStringToFile(new File("avro.schema"), schema.toString(true));
}
  public static void jsonToAvro(File jsonFile, File avroSchema, String avroFilePath) throws Exception {
    Schema schema = Schema.parse(avroSchema);
    File file=new File(avroFilePath);
    file.delete();
    DatumWriter<GenericRecord> writer=new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter=new DataFileWriter<GenericRecord>(writer);
  
    int i = 0;
    dataFileWriter.create(schema, file);
    for (String line : FileUtils.readLines(jsonFile)) {
        JSONObject json = new JSONObject(line);
        GenericRecord datum=new GenericData.Record(schema);
        Iterator keys = json.keys();
        while(keys.hasNext()) {
          String key = (String) keys.next();
          if (key.contains("skill")) {
              System.out.println();
          }
          Object obj = json.get(key);
          if (obj instanceof Integer && schema.getField(key).schema().getType() == Type.LONG) {
            obj = Long.valueOf((Integer)obj);
          } if (obj instanceof String) {
            obj = new Utf8(obj.toString());
          } if (obj instanceof org.json.JSONArray) {
              JSONArray arr = (JSONArray) obj;
              Schema type = schema.getField(key).schema();
              if (schema.getField(key).schema().getType() == Type.UNION) {
                  type =
                      ((Schema) CollectionUtils.find(schema.getField(key).schema().getTypes(), new Predicate() {
                        @Override
                        public boolean evaluate(Object object) {
                          return ((Schema) object).getType() == Type.ARRAY;
                        }
                      }));
                }
              
              GenericData.Array arrAvro = new GenericData.Array(arr.length(), type);
              for (int j = 0; j < arr.length(); j++) {
                  arrAvro.add(arr.get(j));
              }
              obj = arrAvro;
          }
          datum.put(key, obj);
        }
        dataFileWriter.append(datum);
    }
    dataFileWriter.close();
    
  }
}
