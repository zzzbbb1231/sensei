package com.senseidb.ba.gazelle.creators;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ColumnMetadata;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.impl.SortedForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.gazelle.utils.FileSystemMode;
import com.senseidb.ba.gazelle.utils.StreamUtils;

public class SegmentCreator {
  private static Logger logger = Logger.getLogger(SegmentCreator.class);

  public static GazelleIndexSegmentImpl readFromAvroFile(String avroFilePath, FileSystemMode mode, FileSystem fs) throws IOException {
    MetadataCreator creator = new MetadataCreator();
    InputStream inputStream1 = null;
    InputStream inputStream2 = null;
    try {
      Map<String, GazelleForwardIndexImpl> ret =
          new HashMap<String, GazelleForwardIndexImpl>();
      DatumReader<GenericRecord> datumReader =
          new GenericDatumReader<GenericRecord>();
      inputStream1 = StreamUtils.getInputStream(avroFilePath, mode, fs);
      DataFileStream<GenericRecord> dataFileReader =
          new DataFileStream<GenericRecord>(inputStream1, datumReader);

      Schema schema = dataFileReader.getSchema();
      if (dataFileReader.getSchema() == null) {
        throw new IllegalStateException();
      }
      ColumnType[] columnTypes = new ColumnType[schema.getFields().size()];
      String[] columnNames = new String[schema.getFields().size()];
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
        ColumnType columnType = ColumnType.valueOf(type);
        columnTypes[i] = columnType;
        columnNames[i] = field.name();
        i++;
      }
      DictionaryCreator[] creators =
          new DictionaryCreator[schema.getFields().size()];
      for (int j = 0; j < columnTypes.length; j++) {
        creators[j] = new DictionaryCreator();
      }
      int count = 0;
      Iterator<GenericRecord> iterator = dataFileReader.iterator();
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        for (int j = 0; j < columnTypes.length; j++) {

          Object value = record.get(j);
          if (value instanceof Utf8) {
            value = ((Utf8) value).toString();
          }
          creators[j].addValue(value, columnTypes[j]);
        }
        /*
         * if (count % 100000 == 0) { System.out.println("!count = " + count); }
         */
        count++;
      }
      logger.info("Created dictionaries for " + count
          + " elements from the avro file");
      TermValueList[] dictionaries =
          new TermValueList[schema.getFields().size()];
      for (int j = 0; j < columnTypes.length; j++) {
        dictionaries[j] = creators[j].produceDictionary();
      }
      CompressedIntArray[] intArrays =
          new CompressedIntArray[schema.getFields().size()];
      SortedForwardIndexImpl[] sortedForwardIndexes =
          new SortedForwardIndexImpl[schema.getFields().size()];
      for (int j = 0; j < columnTypes.length; j++) {
        if (!creators[j].isSorted()) {
          // System.out.println(columnNames[j] + " = " +
          // dictionaries[j].size());
          intArrays[j] =
              new CompressedIntArray(count, CompressedIntArray.getNumOfBits(dictionaries[j].size()), getByteBuffer(count, dictionaries[j].size()));
        } else {
          sortedForwardIndexes[j] =
              new SortedForwardIndexImpl(dictionaries[j], new int[dictionaries[j].size()], new int[dictionaries[j].size()], count, creator.createMetadata(columnNames[j], dictionaries[j], columnTypes[j], count, true));
        }
      }
      dataFileReader.close();
      datumReader = new GenericDatumReader<GenericRecord>();
      inputStream2 = StreamUtils.getInputStream(avroFilePath, mode, fs);

      dataFileReader =
          new DataFileStream<GenericRecord>(inputStream2, datumReader);

      iterator = dataFileReader.iterator();
      i = 0;
      while (dataFileReader.hasNext()) {
        GenericRecord record = dataFileReader.next();
        for (int j = 0; j < columnTypes.length; j++) {
          if (!creators[j].isSorted()) {
            try {
              intArrays[j].addInt(i, creators[j].getIndex(record.get(j)));
            } catch (Throwable ex) {
              StringBuilder builder = new StringBuilder();
              builder.append("An error has occured: \ncapacity = "
                  + intArrays[j].getCapacity());
              builder.append("\nbitSize = "
                  + CompressedIntArray.getNumOfBits(dictionaries[j].size()));
              builder.append("\nbufferSize = "
                  + CompressedIntArray.getRequiredBufferSize(intArrays[j].getCapacity(), CompressedIntArray.getNumOfBits(dictionaries[j].size())));
              builder.append("\ncounter = " + i);
              logger.error(builder.toString(), ex);
              throw new RuntimeException(ex);
            }
          } else {
            sortedForwardIndexes[j].add(i, creators[j].getIndex(record.get(j)));
          }
        }
        i++;
      }
      dataFileReader.close();
      GazelleIndexSegmentImpl indexSegmentImpl = new GazelleIndexSegmentImpl();
      for (int j = 0; j < columnTypes.length; j++) {
        indexSegmentImpl.getColumnTypes().put(columnNames[j], columnTypes[j]);
        indexSegmentImpl.getDictionaries().put(columnNames[j], dictionaries[j]);

        if (!creators[j].isSorted()) {
          ColumnMetadata metadata =
              creator.createMetadata(columnNames[j], dictionaries[j], columnTypes[j], count, false);
          GazelleForwardIndexImpl forwardIndexImpl =
              new GazelleForwardIndexImpl(columnNames[j], intArrays[j], dictionaries[j], metadata);
          indexSegmentImpl.getColumnMetadataMap().put(columnNames[j], metadata);
          indexSegmentImpl.getForwardIndexes().put(columnNames[j], forwardIndexImpl);
        } else {
          sortedForwardIndexes[j].seal();
          indexSegmentImpl.getForwardIndexes().put(columnNames[j], sortedForwardIndexes[j]);
          indexSegmentImpl.getColumnMetadataMap().put(columnNames[j], sortedForwardIndexes[j].getColumnMetadata());
        }

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

  public static GazelleIndexSegmentImpl readFromAvroFile(String avroFilePath, FileSystemMode mode) throws IOException {
    return readFromAvroFile(avroFilePath, mode, null);
  }

  public static ByteBuffer getByteBuffer(int numOfElements, int dictionarySize) {
    return ByteBuffer.allocate(CompressedIntArray.getRequiredBufferSize(numOfElements, CompressedIntArray.getNumOfBits(dictionarySize)));
  }

}
