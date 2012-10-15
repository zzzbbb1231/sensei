package com.senseidb.ba.gazelle.creators;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.senseidb.ba.ColumnType;
import com.senseidb.ba.ForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;

public class SegmentCreator {
  private static Logger logger = Logger.getLogger(SegmentCreator.class);


  private static GazelleIndexSegmentImpl readFromAvroFile(InputStream inputStream1, InputStream inputStream2) throws IOException {
    try {
      DatumReader<GenericRecord> datumReader =
          new GenericDatumReader<GenericRecord>();
      DataFileStream<GenericRecord> dataFileReader =
          new DataFileStream<GenericRecord>(inputStream1, datumReader);
      Schema schema = dataFileReader.getSchema();
      if (dataFileReader.getSchema() == null) {
        throw new IllegalStateException();
      }
      ForwardIndexCreator[] creators = new  ForwardIndexCreator[schema.getFields().size()];
      int i = 0;
      for (Field field : schema.getFields()) {
        Type type = field.schema().getType();
        if (type == Type.UNION) {
          type =
              ((Schema) CollectionUtils.find(field.schema().getTypes(), new Predicate() {
                @Override
                public boolean evaluate(Object object) {
                  return ((Schema) object).getType() != Type.NULL;
                }
              })).getType();
        }
        ColumnType columnType;
        if (type == Type.ARRAY) {
            columnType = ColumnType.valueOfArrayType(field.schema().getElementType().getType());
        } else {
            columnType = ColumnType.valueOf(type);
        }
        creators[i] = new ForwardIndexCreator(field.name(), columnType);
        i++;
      }
      
      int count = 0;
      Iterator<GenericRecord> iterator = dataFileReader.iterator();
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        for (int j = 0; j < creators.length; j++) {

          Object value = record.get(j);
          if (value instanceof Utf8) {
            value = ((Utf8) value).toString();
          }
          creators[j].addValueToDictionary(value);
        }
        /*
         * if (count % 100000 == 0) { System.out.println("!count = " + count); }
         */
        count++;
      }
      logger.info("Created dictionaries for " + count
          + " elements from the avro file");
      
      for (int j = 0; j < creators.length; j++) {
       creators[j].produceDictionary(count);
      }
      dataFileReader.close();
      datumReader = new GenericDatumReader<GenericRecord>();

      dataFileReader =
          new DataFileStream<GenericRecord>(inputStream2, datumReader);

      iterator = dataFileReader.iterator();
      i = 0;
      while (dataFileReader.hasNext()) {
        GenericRecord record = dataFileReader.next();
        for (int j = 0; j < creators.length; j++) {
          creators[j].addValueToForwardIndex(record.get(j));
        }
        i++;
      }
      dataFileReader.close();
      GazelleIndexSegmentImpl indexSegmentImpl = new GazelleIndexSegmentImpl();
      for (int j = 0; j < creators.length; j++) {
        indexSegmentImpl.getColumnTypes().put(creators[j].getColumnName(), creators[j].getColumnType());
        indexSegmentImpl.getDictionaries().put(creators[j].getColumnName(), creators[j].getDictionary());
        ForwardIndex forwardIndex = creators[j].produceForwardIndex();
        indexSegmentImpl.getColumnMetadataMap().put(creators[j].getColumnName(), creators[j].produceColumnMetadata());
        indexSegmentImpl.getForwardIndexes().put(creators[j].getColumnName(), forwardIndex);
      }
      indexSegmentImpl.setLength(count);
      return indexSegmentImpl;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      IOUtils.closeQuietly(inputStream1);
      IOUtils.closeQuietly(inputStream2);
    }
  }

  public static GazelleIndexSegmentImpl readFromAvroFile(File avroFile) throws IOException {
    return readFromAvroFile(new FileInputStream(avroFile), new FileInputStream(avroFile));
  }
  public static GazelleIndexSegmentImpl readFromAvroFile(String fileSystemPath, FileSystem fileSystem) throws IOException {
      return readFromAvroFile(fileSystem.open(new Path(fileSystemPath)), fileSystem.open(new Path(fileSystemPath)));
    }
  

}
