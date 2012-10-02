package com.senseidb.ba.index1;

import java.io.File;
import java.io.FileInputStream;
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
import org.apache.log4j.Logger;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.DictionaryCreator;
import com.senseidb.ba.IndexSegmentImpl;
import com.senseidb.ba.util.CompressedIntArray;

public abstract class Avro2ForwardIndexMapper {
    private static Logger logger = Logger.getLogger(Avro2ForwardIndexMapper.class);
    
    private final File avroFile;
	
	public Avro2ForwardIndexMapper(File avroFile) {
		this.avroFile = avroFile;
	}
	public IndexSegmentImpl build() throws Exception {
	    FileInputStream inputStream1 = null;
	    FileInputStream inputStream2 = null;
	    try {
	    Map<String, ForwardIndexImpl> ret = new HashMap<String, ForwardIndexImpl>();
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
	    inputStream1 = new FileInputStream(avroFile);
        DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(inputStream1, datumReader);
	    
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
	            type = ((Schema) CollectionUtils.find(field.schema().getTypes(), new Predicate() {
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
	    DictionaryCreator[] creators = new DictionaryCreator[schema.getFields().size()];
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
                creators[j].add(value);
			}
		      count++;
		}
	    TermValueList[] dictionaries = new TermValueList[schema.getFields().size()];
	    for (int j = 0; j < columnTypes.length; j++) {
	    	dictionaries[j] = creators[j].produceDictionary();
		}
	    CompressedIntArray[] intArrays = new CompressedIntArray[schema.getFields().size()];
	    SortedForwardIndexImpl[] sortedForwardIndexes = new SortedForwardIndexImpl[schema.getFields().size()];
	    for (int j = 0; j < columnTypes.length; j++) {
	    	if (!creators[j].isSorted()) {
	    	    intArrays[j] = new CompressedIntArray(count, CompressedIntArray.getNumOfBits(dictionaries[j].size()), getByteBuffer(count, dictionaries[j].size()));
	    	} else {
	    	    sortedForwardIndexes[j] = new SortedForwardIndexImpl(dictionaries[j], new int[dictionaries[j].size()], new int[dictionaries[j].size()], count, getColumnMetadata(dictionaries[j], count, columnNames[j], columnTypes[j], true));
	    	}
	    }
	    dataFileReader.close();
	    datumReader = new GenericDatumReader<GenericRecord>();
	    inputStream2 = new FileInputStream(avroFile);
        
	    dataFileReader = new DataFileStream<GenericRecord>(inputStream2, datumReader);
       
	    iterator = dataFileReader.iterator();
	    i = 0;
	    while (dataFileReader.hasNext()) {
		      GenericRecord record = dataFileReader.next();
		      for (int j = 0; j < columnTypes.length; j++) {
		          if (!creators[j].isSorted()) {
		              intArrays[j].addInt(i, creators[j].getIndex(record.get(j)));
		          } else {
		              sortedForwardIndexes[j].add(i, creators[j].getIndex(record.get(j)));
		          }
			}
		      i++;
		}
	    dataFileReader.close();
	    IndexSegmentImpl indexSegmentImpl = new IndexSegmentImpl();
	    for (int j = 0; j < columnTypes.length; j++) {
	    	indexSegmentImpl.getColumnTypes().put(columnNames[j], columnTypes[j]);
	    	indexSegmentImpl.getDictionaries().put(columnNames[j], dictionaries[j]);
	    	if (!creators[j].isSorted()) {
	    	    indexSegmentImpl.getForwardIndexes().put(columnNames[j], new ForwardIndexImpl(columnNames[j], intArrays[j], dictionaries[j], getColumnMetadata(dictionaries[j], count, columnNames[j], columnTypes[j], false)));
	    	} else {
	    	    sortedForwardIndexes[j].seal();
	    	    indexSegmentImpl.getForwardIndexes().put(columnNames[j], sortedForwardIndexes[j]);
	    	}
		}
	    indexSegmentImpl.setLength(count);
	    return indexSegmentImpl;
	    } catch (Exception ex) {
	      logger.error(ex.getMessage(), ex);
	      throw new RuntimeException(ex);
	    } finally {
	        IOUtils.closeQuietly(inputStream1);
	        IOUtils.closeQuietly(inputStream2);
	    }
	}
	public abstract ColumnMetadata getColumnMetadata(TermValueList dictionaries, int count, String columnName, ColumnType columnType, boolean isSorted);
	public abstract ByteBuffer getByteBuffer(int numOfElements, int dictionarySize);
	

}
