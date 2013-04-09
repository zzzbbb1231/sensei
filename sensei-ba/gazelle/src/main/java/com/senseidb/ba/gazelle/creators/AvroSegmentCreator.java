package com.senseidb.ba.gazelle.creators;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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

import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.custom.CompositeMetricCustomIndex;
import com.senseidb.ba.gazelle.custom.GazelleCustomIndex;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.gazelle.utils.FileSystemMode;

public class AvroSegmentCreator {
  private static Logger logger = Logger.getLogger(AvroSegmentCreator.class);


  private static GazelleIndexSegmentImpl readFromAvroFile(DataFileStream<GenericRecord> stream1, DataFileStream<GenericRecord> stream2, Map<String, GazelleCustomIndex> customIndexes) throws IOException {
    try {
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
     
      Schema schema = stream1.getSchema();
      if (stream1.getSchema() == null) {
        throw new IllegalStateException();
      }
      ForwardIndexCreator[] creators = new  ForwardIndexCreator[schema.getFields().size()];
      int i = 0;
      GazelleCustomIndex[] customIndexArray = new  GazelleCustomIndex[schema.getFields().size()];
      for (Field field : schema.getFields()) {
        if (customIndexes.get(field.name()) != null) {
          customIndexArray[i] = customIndexes.get(field.name());
        } 
        ColumnType columnType = getColumnType(field);
        creators[i] = new ForwardIndexCreator(field.name(), columnType);
        i++;
      }
      
      int count = 0;
      Iterator<GenericRecord> iterator = stream1.iterator();
      while (iterator.hasNext()) {
        GenericRecord record = iterator.next();
        for (int j = 0; j < creators.length; j++) {

          Object value = record.get(j);
          if (value instanceof Utf8) {
            value = ((Utf8) value).toString();
          }
          if (customIndexArray[j] != null) {
            customIndexArray[j].getCreator().addValueToDictionary(creators[j].getColumnName(), value);
          } else {
            creators[j].addValueToDictionary(value);
          }
        }
        /*
         * if (count % 100000 == 0) { System.out.println("!count = " + count); }
         */
        count++;
      }
      logger.info("Created dictionaries for " + count + " elements from the avro file");
      
      for (int j = 0; j < creators.length; j++) {
        if (customIndexArray[j] != null) {
          customIndexArray[j].getCreator().buildDictionary();
        } else {
          creators[j].produceDictionary(count);
        }
      }
      stream1.close();
     

      iterator = stream2.iterator();
      i = 0;
      while (stream2.hasNext()) {
       /* if (i%100000 == 0)
        System.out.println(" Added " + i + " elements to the forwardIndex");*/
        GenericRecord record = stream2.next();
        for (int j = 0; j < creators.length; j++) {
          if (customIndexArray[j] != null) {
            customIndexArray[j].getCreator().newDocumentOnForwardIndex(i);
            customIndexArray[j].getCreator().addToForwardIndex(creators[j].getColumnName(), record.get(j));
          } else {
            creators[j].addValueToForwardIndex(record.get(j));
          }
          
        }
        i++;
        
       }
      stream2.close();
      GazelleIndexSegmentImpl indexSegmentImpl = new GazelleIndexSegmentImpl();
      for (int j = 0; j < creators.length; j++) {
        if (customIndexArray[j] != null) {
          indexSegmentImpl.addCustomIndex(customIndexArray[j], creators[j].getColumnName());
        } else {
          
          indexSegmentImpl.getColumnTypes().put(creators[j].getColumnName(), creators[j].getColumnType());
          indexSegmentImpl.getDictionaries().put(creators[j].getColumnName(), creators[j].getDictionary());
          ForwardIndex forwardIndex = creators[j].produceForwardIndex();
          indexSegmentImpl.getColumnMetadataMap().put(creators[j].getColumnName(), creators[j].produceColumnMetadata());
          indexSegmentImpl.getForwardIndexes().put(creators[j].getColumnName(), forwardIndex);
        }
      }
      indexSegmentImpl.setLength(count);
      return indexSegmentImpl;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      
    }
  }

  public static ColumnType getColumnType(Field field) {
    Schema fieldSchema = field.schema();
    fieldSchema = extractSchemaFromUnionIfNeeded(fieldSchema);
    ColumnType columnType;
    Type type = fieldSchema.getType();
    if (type == Type.ARRAY) {
        Schema elementSchema = extractSchemaFromUnionIfNeeded(fieldSchema.getElementType());
        if (elementSchema.getType() == Type.RECORD) {
          if (elementSchema.getField("token") != null) {
            elementSchema = elementSchema.getField("token").schema();
          } else {
            elementSchema = elementSchema.getField("null").schema();
          }
          elementSchema = extractSchemaFromUnionIfNeeded(elementSchema);
        }
        columnType = ColumnType.valueOfArrayType( elementSchema.getType());
        
    } else {
        columnType = ColumnType.valueOf(type);
    }
    return columnType;
  }

  public static Schema extractSchemaFromUnionIfNeeded(Schema fieldSchema) {
    if (fieldSchema.getType() == Type.UNION) {
        fieldSchema =
          ((Schema) CollectionUtils.find(fieldSchema.getTypes(), new Predicate() {
            @Override
            public boolean evaluate(Object object) {
              return ((Schema) object).getType() != Type.NULL;
            }
          }));
    }
    return fieldSchema;
  }

 
  public static GazelleIndexSegmentImpl readFromAvroFile(InputStream fileInputStream, InputStream fileInputStream2) throws IOException {
    try {
      DatumReader<GenericRecord> datumReader1 = new GenericDatumReader<GenericRecord>();
      DataFileStream<GenericRecord> dataFileReader1 = new DataFileStream<GenericRecord>(fileInputStream, datumReader1);
      Schema schema = dataFileReader1.getSchema();
      
      DatumReader<GenericRecord> datumReader2 =  new GenericDatumReader<GenericRecord>();
      DataFileStream<GenericRecord> dataFileReader2 = new DataFileStream<GenericRecord>(fileInputStream2, datumReader2);
      return readFromAvroFile(dataFileReader1, dataFileReader2, new HashMap<String, GazelleCustomIndex>());
      
    } finally {
      IOUtils.closeQuietly(fileInputStream);
      IOUtils.closeQuietly(fileInputStream2);
    }
  }
  
  public static class CreateSegmentInfo {
    InputStream fileInputStream; 
    InputStream fileInputStream2; 
    String baseDir; 
    FileSystemMode mode; 
    FileSystem fs; 
    Map<String,String> additionalProperties = new HashMap<String, String>(); 
    List<String> compositeMetrics;
     public CreateSegmentInfo setAvroSegment(InputStream fileInputStream, InputStream fileInputStream2)  {
      this.fileInputStream = fileInputStream;
      this.fileInputStream2 = fileInputStream2;
       return this;
     }
     public CreateSegmentInfo setAvroSegment(File avroFile)  {
       try {
        this.fileInputStream = new FileInputStream(avroFile);
        this.fileInputStream2 = new FileInputStream(avroFile);
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
       
        return this;
      }
    public CreateSegmentInfo setOutputDirInfo(String baseDir, FileSystemMode mode, FileSystem fs) {
      this.baseDir = baseDir;
      this.mode = mode;
      this.fs = fs;
      return this;
    } 
    
    public CreateSegmentInfo setAdditionalProperties(Map<String, String> additionalProperties) {
      this.additionalProperties = additionalProperties;
      return this;
    }
    public CreateSegmentInfo setCompositeMetrics(List<String> compositeMetrics) {
      this.compositeMetrics = compositeMetrics;
      return this;
    }
    
    
  }
  
  public static void flushSegmentFromAvroFileWithCompositeMetrics(CreateSegmentInfo bean) throws IOException {
    try {
      DatumReader<GenericRecord> datumReader1 = new GenericDatumReader<GenericRecord>();
      DataFileStream<GenericRecord> dataFileReader1 = new DataFileStream<GenericRecord>(bean.fileInputStream, datumReader1);
      Schema schema = dataFileReader1.getSchema();
      
      DatumReader<GenericRecord> datumReader2 =  new GenericDatumReader<GenericRecord>();
      DataFileStream<GenericRecord> dataFileReader2 = new DataFileStream<GenericRecord>(bean.fileInputStream2, datumReader2);
      
    HashMap<String, GazelleCustomIndex> customIndexes = new HashMap<String, GazelleCustomIndex>();
    CompositeMetricCustomIndex compositeMetricCustomIndex = new CompositeMetricCustomIndex();
    List<String> columnNames = new ArrayList<String>();
    for (Field field : schema.getFields()) {
      columnNames.add(field.name());
    }
    Map<String, ColumnMetadata> compositeMetadata = new HashMap<String, ColumnMetadata>();
    if (bean.compositeMetrics != null) {
    for (String metric : bean.compositeMetrics) {
      for (String column : matchByWildCard(columnNames, metric)) {
        customIndexes.put(column, compositeMetricCustomIndex);
        compositeMetadata.put(column, new ColumnMetadata(column,getColumnType(schema.getField(column))));
      }
    }
    }
    compositeMetricCustomIndex.init(compositeMetadata);
    compositeMetricCustomIndex.getCreator().init(bean.baseDir, bean.mode, bean.fs);
    GazelleIndexSegmentImpl indexSegmentImpl = readFromAvroFile(dataFileReader1, dataFileReader2, customIndexes);
    
    if (bean.additionalProperties != null) {
      for (String key : bean.additionalProperties.keySet()) {
        indexSegmentImpl.getSegmentMetadata().put(key, bean.additionalProperties.get(key));
      }
    }
    if (bean.mode == FileSystemMode.HDFS) {
      SegmentPersistentManager.flushToHadoop(indexSegmentImpl, bean.baseDir, bean.fs);
    } else {
      SegmentPersistentManager.flushToDisk(indexSegmentImpl, new File(bean.baseDir));
    }
    
    } finally {
      IOUtils.closeQuietly(bean.fileInputStream);
      IOUtils.closeQuietly(bean.fileInputStream2);
    }
  }
  public static Set<String> matchByWildCard(List<String> fields, String expression) {
    Set<String> ret = new HashSet<String>();
    expression = expression.trim();
    if (expression.contains("*")) {
      String[] parts = expression.split("\\*");
      for (String field : fields) {
        if (match(field, parts)) {
          ret.add(field);
        }
      }
    } else {
      for (String field : fields) {
        if (field.equalsIgnoreCase(expression)) {
          ret.add(field);
        }
      }
    }
    return ret;
  }
  public  static boolean match(String str, String[]parts) {
    int index = 0;
    for(String part : parts) {
      if (part == null || part.length() == 0) {
        continue;
      }
      index = str.indexOf(part, index);
      if (index < 0) {
        return false;
      }
      index += part.length();
      if (index >= str.length() ){
         return false; 
      }
      }
    return true;
  }
  public static GazelleIndexSegmentImpl readFromAvroFile(String fileSystemPath, FileSystem fileSystem) throws IOException {
      return readFromAvroFile(fileSystem.open(new Path(fileSystemPath)), fileSystem.open(new Path(fileSystemPath)));
    }

  public static GazelleIndexSegmentImpl readFromAvroFile(File file) {
    
    try {
      return readFromAvroFile(new FileInputStream(file), new FileInputStream(file));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  

}
