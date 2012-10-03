package com.linkedin.gazelle.creators;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

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
import org.apache.log4j.Logger;
import org.mortbay.io.RuntimeIOException;

import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.dao.GazelleIndexSegmentImpl;
import com.linkedin.gazelle.utils.GazelleColumnMetadata;
import com.linkedin.gazelle.utils.GazelleColumnType;
import com.linkedin.gazelle.utils.CompressedIntArray;

public class SegmentCreator {
  private static Logger logger = Logger.getLogger(SegmentCreator.class);

  private String[] _columnNames;
  private File _avroFile;
  private DatumReader<GenericRecord> _datumReader;
  private DataFileStream<GenericRecord> _dataFileReader;
  private Schema _schema;
  private GazelleColumnMetadata[] _columnMetadataArr;
  private DictionaryCreator[] _dictionaryWriterArr;
  private TermValueList[] _termValueLists;
  private CompressedIntArray[] _compressedIntArr;
  private int _numOfElements;

  private void intializeColumnMedataAndDictionaryArrs() throws IOException {
    FileInputStream fs = null;
    try {
      fs = new FileInputStream(_avroFile);
      _dataFileReader = new DataFileStream<GenericRecord>(fs, _datumReader);
      _schema = _dataFileReader.getSchema();
      if (_schema == null) {
        throw new IllegalStateException("Cannot read the Schema File");
      }
      int i = 0;
      _columnMetadataArr = new GazelleColumnMetadata[_schema.getFields().size()];
      _dictionaryWriterArr = new DictionaryCreator[_schema.getFields().size()];
      for (Field field : _schema.getFields()) {
        Type type = field.schema().getType();
        if (type == Type.UNION) {
          type = ((Schema) CollectionUtils.find(field.schema().getTypes(), new Predicate() {
            @Override
            public boolean evaluate(Object object) {
              return ((Schema) object).getType() != Type.NULL;
            }
          })).getType();
          _columnMetadataArr[i] = new GazelleColumnMetadata(field.name(), GazelleColumnType.getType(type.toString()));
          _dictionaryWriterArr[i] = new DictionaryCreator();
          i++;
        }
      }
    } finally {
      fs.close();
    }
  }

  public GazelleIndexSegmentImpl process(File file) throws IOException {
    _avroFile = file;
    _datumReader = new GenericDatumReader<GenericRecord>();
    intializeColumnMedataAndDictionaryArrs();
    _termValueLists = new TermValueList[_schema.getFields().size()];
    _compressedIntArr = new CompressedIntArray[_schema.getFields().size()];
    try {
      _numOfElements = 1;
      FileInputStream fs1 = new FileInputStream(_avroFile);
      _dataFileReader = new DataFileStream<GenericRecord>(fs1, _datumReader);
      while (_dataFileReader.hasNext()) {
        GenericRecord record = _dataFileReader.next();
        if (record != null) {
          for (int i = 0; i < _columnMetadataArr.length; i++) {
            Object columnEntry = record.get(_columnMetadataArr[i].getName());
            if (columnEntry instanceof Utf8) {
              columnEntry = ((Utf8) columnEntry).toString();
            }
            _dictionaryWriterArr[i].addValue(columnEntry, _columnMetadataArr[i].getColumnType());
          }
          _numOfElements++;
        } else {
          logger.info("null record found");
        }
      }

      for (int i = 0; i < _columnMetadataArr.length; i++) {
        _termValueLists[i] = _dictionaryWriterArr[i].getTermValueList(_columnMetadataArr[i].getColumnType());
        _compressedIntArr[i] =
            new CompressedIntArray(_numOfElements, CompressedIntArray.getNumOfBits(_termValueLists[i].size()),
                getByteBuffer(_numOfElements, _termValueLists[i].size()));
      }
      _dataFileReader.close();
      fs1.close();
      FileInputStream fs2 = new FileInputStream(_avroFile);
      _dataFileReader = new DataFileStream<GenericRecord>(fs2, _datumReader);
      int incrementor = 0;
      while (_dataFileReader.hasNext()) {
        GenericRecord record = _dataFileReader.next();
        for (int i = 0; i < _columnMetadataArr.length; i++) {
          Object value = record.get(i);
          if (value instanceof Utf8) {
            value = ((Utf8) value).toString();
          }
          _compressedIntArr[i].addInt(incrementor, _dictionaryWriterArr[i].getValue(value, _columnMetadataArr[i].getColumnType()));
        }
        incrementor++;
      }
      return new GazelleIndexSegmentImpl(_compressedIntArr, _termValueLists, _columnMetadataArr, _numOfElements);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  public ByteBuffer getByteBuffer(int numOfElements, int dictionarySize) {
    return ByteBuffer.allocate(CompressedIntArray.getRequiredBufferSize(numOfElements,
        CompressedIntArray.getNumOfBits(dictionarySize)));
  }

}
