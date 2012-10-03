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
import com.linkedin.gazelle.creators.SegmentCreator;
import com.linkedin.gazelle.dao.GazelleIndexSegmentImpl;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.DictionaryCreator;
import com.senseidb.ba.IndexSegmentImpl;
import com.senseidb.ba.util.CompressedIntArray;

public abstract class Avro2ForwardIndexMapper {
    private static Logger logger = Logger.getLogger(Avro2ForwardIndexMapper.class);
    
    protected final File avroFile;
	
	public Avro2ForwardIndexMapper(File avroFile) {
		this.avroFile = avroFile;
	}
	public GazelleIndexSegmentImpl build() throws Exception {
	   return new SegmentCreator().process(avroFile);
	}
	public abstract ColumnMetadata getColumnMetadata(TermValueList dictionaries, int count, String columnName, ColumnType columnType, boolean isSorted);
	public abstract ByteBuffer getByteBuffer(int numOfElements, int dictionarySize);
	

}
