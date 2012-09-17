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

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.DictionaryCreator;
import com.senseidb.ba.IndexSegmentImpl;
import com.senseidb.ba.util.CompressedIntArray;

public abstract class Avro2ForwardIndexMapper {
	private final File avroFile;
	
	public Avro2ForwardIndexMapper(File avroFile) {
		this.avroFile = avroFile;
	}
	public IndexSegmentImpl build() throws Exception {
		Map<String, ForwardIndexImpl> ret = new HashMap<String, ForwardIndexImpl>();
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
	    DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(new FileInputStream(avroFile), datumReader);
	    
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
				if ("dim_memberEducation".equalsIgnoreCase(columnNames[j])) {
				    //System.out.println("!!!" + value);
				}
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
	    for (int j = 0; j < columnTypes.length; j++) {
	    	intArrays[j] = new CompressedIntArray(count, CompressedIntArray.getNumOfBits(dictionaries[j].size()), getByteBuffer(count, dictionaries[j].size()));
		}
	    dataFileReader.close();
	    datumReader = new GenericDatumReader<GenericRecord>();
         dataFileReader = new DataFileStream<GenericRecord>(new FileInputStream(avroFile), datumReader);
       
	    iterator = dataFileReader.iterator();
	    i = 0;
	    while (dataFileReader.hasNext()) {
		      GenericRecord record = dataFileReader.next();
		      for (int j = 0; j < columnTypes.length; j++) {
		    	  intArrays[j].addInt(i, creators[j].getIndex(record.get(j)));
			}
		      i++;
		}
	    dataFileReader.close();
	    IndexSegmentImpl indexSegmentImpl = new IndexSegmentImpl();
	    for (int j = 0; j < columnTypes.length; j++) {
	    	indexSegmentImpl.getColumnTypes().put(columnNames[j], columnTypes[j]);
	    	indexSegmentImpl.getDictionaries().put(columnNames[j], dictionaries[j]);
	    	indexSegmentImpl.getForwardIndexes().put(columnNames[j], new ForwardIndexImpl(columnNames[j], intArrays[j], dictionaries[j], getColumnMetadata(dictionaries[j], count, columnNames[j], columnTypes[j])));
		}
	    indexSegmentImpl.setLength(count);
	    return indexSegmentImpl;
	}
	public abstract ColumnMetadata getColumnMetadata(TermValueList dictionaries, int count, String columnName, ColumnType columnType);
	public abstract ByteBuffer getByteBuffer(int numOfElements, int dictionarySize);
	

}
