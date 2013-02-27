package com.senseidb.ba.realtime.dataproviders;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.springframework.util.Assert;

import com.senseidb.ba.gazelle.creators.ForwardIndexCreator;
import com.senseidb.ba.realtime.Schema;
import com.senseidb.ba.realtime.indexing.DataWithVersion;
import com.senseidb.ba.realtime.indexing.RealtimeDataProvider;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;

public class AvroFileDataProvider implements RealtimeDataProvider, SenseiPlugin {
  int position = -1;
  private Schema schema;
  private DataFileStream<GenericRecord> dataFileReader;
  private Iterator<GenericRecord> iterator;
  private Iterator<File> fileIterator;
  private boolean isFinished = false;
  private FileInputStream fileInputStream;

  @Override
  public void init(Schema schema, String lastVersion) {
    this.schema = schema;

  }

  @Override
  public DataWithVersion next() {
    if (fileIterator == null) {
      return null;
    }
    if (isFinished) {
      return null;
    }
    try {
      if (iterator != null && iterator.hasNext()) {
        GenericRecord genericRecord = iterator.next();
        return getNextData(genericRecord);
      } else if (fileIterator.hasNext()) {
        if (dataFileReader != null) {
          dataFileReader.close();
        }
        if (fileInputStream != null) {
          fileInputStream.close();
        }
        File file = fileIterator.next();
        fileInputStream = new FileInputStream(file);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        dataFileReader = new DataFileStream<GenericRecord>(fileInputStream, datumReader);
        iterator = dataFileReader.iterator();
        return next();
      } else {
        if (dataFileReader != null) {
          dataFileReader.close();
        }
        if (fileInputStream != null) {
          fileInputStream.close();
        }
        isFinished = true;
        return next();
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

  }

  public DataWithVersion getNextData(GenericRecord genericRecord) {
    position++;
    final Object[] values = new Object[schema.getColumnNames().length];
    int i = 0;
    for (String column : schema.getColumnNames()) {
      Object obj = genericRecord.get(column);
      if (obj instanceof Utf8) {
        obj = obj.toString();
      }
      if (obj instanceof Array) {
        obj = ForwardIndexCreator.transform((Array) obj);
      }
      values[i] = obj;
      i++;
    }
    return new DataWithVersion() {

      @Override
      public String getVersion() {
        return "" + position;
      }

      @Override
      public Object[] getValues() {
        return values;
      }
    };
  }

  @Override
  public void commit(String version) {

  }

  @Override
  public void startProvider() {

  }

  @Override
  public void stopProvider() {

  }

  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    String filePath = config.get("file.path");
    Assert.notNull(filePath, "filePath parameter shoud be defined");
    File file = new File(filePath);
    List<File> filesToIterate = new ArrayList<File>();
    if (file.isDirectory()) {
      for (File avroFile : file.listFiles()) {
        //if (file.getName().toLowerCase().endsWith("avro")) {
          filesToIterate.add(avroFile);
        //}
      }
    } else {
      filesToIterate.add(file);
    }
    fileIterator = filesToIterate.iterator();

  }

  @Override
  public void start() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub
    
  }

}
