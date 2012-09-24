package com.senseidb.ba;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.trevni.ColumnFileWriter;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ValueType;
import org.apache.avro.Schema.Type;


/**
 * @author dpatel
 */

public class DataMaker {
  private static ColumnMetaData[] columnMetaDataArr;
  private static Schema schema;
  private static ColumnFileWriter columnFileWriter;
  private static Map<String, Map<String, Integer>> dictionaries;
  private static Map<String, Integer> highestDictMappingInts;
  private static Map<String, Map<String, Integer>> dictValueFrequencies;
  private static String rootOutputDir;
  private static long numRowsInWriter;

  private static String shardingField;
  private static String timeField;
  private static String sortingField;
  private static Object timeFieldValue;
  private static Object beginShardingFieldValue;
  private static Object endShardingFieldValue;
  
  public static Schema createTrevniFilesForAndReturnSchema(File avroFile, String dir) throws IOException {
    InputStream inStream = new FileInputStream(avroFile);
    rootOutputDir = dir;
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(inStream,
        datumReader);
    if (dataFileReader.getSchema() != null) {
      schema = dataFileReader.getSchema();
    }
    setUpTrevniMeatadatForIndexing();
    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();
      writeRecord(record);
    }
    flushDictionaries();
    flushIndexFile();
    return schema;
  }

  
  private static void flushIndexFile() throws IOException {
    writeFileMetaData();
    writeDimsMetaData();
    writeDictsMetaData();
    columnFileWriter.writeTo(new File(rootOutputDir + "/test-" + timeFieldValue.toString() + ".trv"));
  }

  private static void writeFileMetaData() {
    columnFileWriter.getMetaData().set("source", "test");
    columnFileWriter.getMetaData().set("time", timeFieldValue.toString());
    columnFileWriter.getMetaData().set("shardDim", shardingField);
    columnFileWriter.getMetaData().set("shardValueBegin", beginShardingFieldValue.toString());
    columnFileWriter.getMetaData().set("shardValueEnd", endShardingFieldValue.toString());
    if (sortingField != null) {
      columnFileWriter.getMetaData().set("sortDim", sortingField.toString());
    }
  }

  private static void writeDimsMetaData() {
    if (columnMetaDataArr != null && columnMetaDataArr.length > 0) {
      String initFieldName = columnMetaDataArr[0].getName();
      StringBuilder colNames = new StringBuilder(initFieldName);
      StringBuilder dimTypes = new StringBuilder(initFieldName + ":"
          + getTypeFromAvroSchema(schema, initFieldName).toString());
      for (int i = 1; i < columnMetaDataArr.length; i++) {
        String fieldName = columnMetaDataArr[i].getName();
        colNames.append("," + fieldName);
        dimTypes
            .append("," + fieldName + ":" + getTypeFromAvroSchema(schema, fieldName).toString());
      }
      columnFileWriter.getMetaData().set("orderedDimNames", colNames.toString());
      columnFileWriter.getMetaData().set("dimTypes", dimTypes.toString());
    }
  }

  private static void writeDictsMetaData() {
    String[] dictNames = dictionaries.keySet().toArray(new String[0]);
    if (dictNames != null && dictNames.length > 0) {
      StringBuilder dictMapping = new StringBuilder(dictNames[0] + ":" + dictNames[0] + "-"
          + "test" + "-" + timeFieldValue + ".dict");
      for (int i = 1; i < dictNames.length; i++) {
        dictMapping.append("," + dictNames[i] + ":" + dictNames[i]
            + "-" + "test" + "-" + timeFieldValue + ".dict");
      }
      columnFileWriter.getMetaData()
          .set("dictMapping", dictMapping.toString());
    }
  }

  private static void flushDictionaries() throws IOException {
    for (Map.Entry<String, Map<String, Integer>> entry : dictionaries.entrySet()) {
      ColumnFileWriter columnDictFileWriter = new ColumnFileWriter(
          new org.apache.trevni.ColumnFileMetaData().setChecksum("null").setCodec("null"),
          new ColumnMetaData(entry.getKey(), ValueType.STRING));

      columnDictFileWriter.getMetaData().set("mappedType",
          getTypeFromAvroSchema(schema, entry.getKey()).toString());
      for (Map.Entry<String, Integer> columnIdxEntry : entry.getValue().entrySet()) {
        columnDictFileWriter.writeRow(columnIdxEntry.getKey() + ":"
            + columnIdxEntry.getValue().toString() + ":"
            + dictValueFrequencies.get(entry.getKey()).get(columnIdxEntry.getKey()));
      }
      String path = rootOutputDir+"/" + entry.getKey() + "-" + "test" + "-" + timeFieldValue + ".dict";
      columnDictFileWriter.writeTo(new File(path));
    }
  }

  public static Type getTypeFromAvroSchema(Schema schema, String fieldName) {
    List<Schema> types = schema.getField(fieldName).schema().getTypes();
    // Avro schema's type is a union schema
    for (Schema type : types) {
      if (!type.getType().equals(Type.NULL)) {
        return type.getType();
      }
    }
    return null;
  }

  private static void writeRecord(GenericRecord record) throws IOException {
    if (beginShardingFieldValue == null) {
      beginShardingFieldValue = record.get(shardingField);
    }
    endShardingFieldValue = record.get(shardingField);
    Object[] trevniRow = getTrevniRow(record);
    columnFileWriter.writeRow(trevniRow);
    numRowsInWriter += 1;
    timeFieldValue = record.get(timeField);
  }

  private static Object[] getTrevniRow(GenericRecord record) {

    Object[] trevniRow = new Object[columnMetaDataArr.length];
    for (int i = 0; i < columnMetaDataArr.length; i++) {
      String columnName = columnMetaDataArr[i].getName();
      Object valueToIndex = null;
      if (shouldCreateDictionary(columnName)) {
        String value = record.get(columnName).toString();
        if (value.equals("")) {
          dictionaries.get(columnName).put("", 0);
          valueToIndex = 0;
        } else {
          valueToIndex = dictionaries.get(columnName).get(value);
          if (valueToIndex == null) {
            valueToIndex = highestDictMappingInts.get(columnName) + 1;
            highestDictMappingInts.put(columnName, (Integer) valueToIndex);
            dictionaries.get(columnName).put(value, (Integer) valueToIndex);
          }
        }

        Integer frequency = dictValueFrequencies.get(columnName).get(value);
        if (frequency == null) {
          dictValueFrequencies.get(columnName).put(value, 1);
        } else {
          dictValueFrequencies.get(columnName).put(value, frequency + 1);
        }

      } else {
        if (columnName.startsWith("met_")) {
          valueToIndex = Double.parseDouble(record.get(columnName).toString());
        } else if (columnName.startsWith("time_")) {
          valueToIndex = Long.parseLong(record.get(columnName).toString());
        }
      }
      trevniRow[i] = valueToIndex;
    }
    return trevniRow;
  }

  private static void setUpTrevniMeatadatForIndexing() throws IOException {
    dictionaries = new HashMap<String, Map<String, Integer>>();
    highestDictMappingInts = new HashMap<String, Integer>();
    dictValueFrequencies = new HashMap<String, Map<String, Integer>>();
    initializeColumnMetaData();
    columnFileWriter = new ColumnFileWriter(new org.apache.trevni.ColumnFileMetaData().setChecksum(
        "null").setCodec("null"), columnMetaDataArr);
    for (ColumnMetaData cmd : columnMetaDataArr) {
      if (shouldCreateDictionary(cmd.getName())) {
        dictionaries.put(cmd.getName(), new HashMap<String, Integer>());
        highestDictMappingInts.put(cmd.getName(), 0);
        dictValueFrequencies.put(cmd.getName(), new HashMap<String, Integer>());
      }
    }
  }

  private static void initializeColumnMetaData() {
    List<Field> fields = schema.getFields();
    List<ColumnMetaData> cMetadataList = new ArrayList<ColumnMetaData>();
    for (int i = 0; i < fields.size(); i++) {
      String currFieldName = fields.get(i).name();
      ValueType type = null;
      if (currFieldName != null) {
        type = getIndexColumnValueType(currFieldName);
      }
      if (type != null) {
        ColumnMetaData mData = new ColumnMetaData(currFieldName, type);
        if (currFieldName.startsWith("shrd_")) {
          mData.hasIndexValues(true);
          cMetadataList.add(0, mData);
        } else if (currFieldName.startsWith("sort_")) {
          if (shardingField == null) {
            cMetadataList.add(0, mData);
          } else {
            cMetadataList.add(1, mData);
          }
        } else {
          cMetadataList.add(mData);
        }
      }
    }
    columnMetaDataArr = cMetadataList.toArray(new ColumnMetaData[0]);
  }

  private static ValueType getIndexColumnValueType(String fieldName) {
    if (fieldName.startsWith("dim_")) {
      return ValueType.INT;
    } else if (fieldName.startsWith("met_")) {
      return ValueType.DOUBLE;
    } else if (fieldName.startsWith("time_")) {
      timeField = fieldName;
      return ValueType.LONG;
    } else if (fieldName.startsWith("shrd_")) {
      shardingField = fieldName;
      return ValueType.INT;
    } else if (fieldName.startsWith("sort_")) {
      sortingField = fieldName;
      return ValueType.INT;
    }
    return null;
  }

  private static boolean shouldCreateDictionary(String fieldName) {
    return fieldName.startsWith("shrd_") || fieldName.startsWith("sort_")
        || fieldName.startsWith("dim_");
  }
}
