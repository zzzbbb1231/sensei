package com.senseidb.ba.realtime;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;

import com.senseidb.ba.gazelle.creators.ForwardIndexCreator;
import com.senseidb.ba.realtime.indexing.DataWithVersion;
import com.senseidb.ba.realtime.indexing.RealtimeDataProvider;
import com.senseidb.ba.util.TestUtil;

public class BaSentinelDataProvider implements RealtimeDataProvider{
  int position  = -1;
  private Schema schema;
  private DataFileStream<GenericRecord> dataFileReader;
  private Iterator<GenericRecord> iterator;
  @Override
  public void init(Schema schema, String lastVersion) {
    this.schema = schema;
   
    refreshIterator();
   
  }

  public void refreshIterator() {
    try { 
    File avroFile = new File(TestUtil.class.getClassLoader().getResource("data/sample_data.avro").toURI());
    FileInputStream fileInputStream = new FileInputStream(avroFile);
    DatumReader<GenericRecord> datumReader =
        new GenericDatumReader<GenericRecord>();
   
    dataFileReader = new DataFileStream<GenericRecord>(fileInputStream, datumReader);
    iterator = dataFileReader.iterator();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DataWithVersion next() {
    if (!iterator.hasNext() && position < 11000) {
      refreshIterator();
    }
    if (!iterator.hasNext()) {
      return null;
    }
    position++;
    if (position % 500 == 0) {
      System.out.println("!!!" + position);
    }
    final Object[] values = new Object[schema.getColumnNames().length];
    GenericRecord genericRecord = iterator.next();
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
        // TODO Auto-generated method stub
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
    // TODO Auto-generated method stub
    
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
