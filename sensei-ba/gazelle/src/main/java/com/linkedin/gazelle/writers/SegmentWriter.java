package com.linkedin.gazelle.writers;

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

import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.Segment;
import com.linkedin.gazelle.utils.ColumnMedata;
import com.linkedin.gazelle.utils.ColumnType;
import com.linkedin.gazelle.utils.CompressedIntArray;

public class SegmentWriter {
  private static Logger logger = Logger.getLogger(SegmentWriter.class);
  
  private String[] _columnNames;
  private File _avroFile;
  private DatumReader<GenericRecord> _datumReader;
  private DataFileStream<GenericRecord> _dataFileReader;
  private Schema _schema;
  private ColumnMedata[] _columnMetadataArr;
  private DictionaryWriter[] _dictionaryWriterArr;
  private TermValueList[] _termValueLists;
  private CompressedIntArray[] _compressedIntArr;
  private ForwardIndexWriter _forwardIndexWriter;
  private MetadataWriter _metadataWriter;
  private int _numOfElements;

  private HashMap<String, CompressedIntArray> _compressedIntArrMap;
  private HashMap<String, TermValueList> _termValueListMap;
  private HashMap<String, ColumnMedata> _metadataMap;

  public SegmentWriter(File file) {
    _avroFile = file;
    _datumReader = new GenericDatumReader<GenericRecord>();
    intializeColumnMedataAndDictionaryArrs();
    _termValueLists = new TermValueList[_schema.getFields().size()];
    _compressedIntArr = new CompressedIntArray[_schema.getFields().size()];
  }

  private void intializeColumnMedataAndDictionaryArrs() {
    try {
      FileInputStream fs = new FileInputStream(_avroFile);
      _dataFileReader = new DataFileStream<GenericRecord>(fs, _datumReader);
      _schema = _dataFileReader.getSchema();
      if (_schema == null) {
        throw new IllegalStateException("Cannot read the Schema File");
      }
      int i = 0;
      _columnMetadataArr = new ColumnMedata[_schema.getFields().size()];
      _dictionaryWriterArr = new DictionaryWriter[_schema.getFields().size()];
      for (Field field : _schema.getFields()) {
        Type type = field.schema().getType();
        if (type == Type.UNION) {
          type =
              ((Schema) CollectionUtils.find(field.schema().getTypes(), new Predicate() {
                @Override
                public boolean evaluate(Object object) {
                  return ((Schema) object).getType() != Type.NULL;
                }
              })).getType();
          _columnMetadataArr[i] =
              new ColumnMedata(field.name(), ColumnType.getType(type.toString()));
          _dictionaryWriterArr[i] = new DictionaryWriter(_columnMetadataArr[i]);
          i++;
        }
      }
    } catch (FileNotFoundException e) {
      logger.error(e);
    } catch (IOException e) {
      logger.error(e);
    }
  }

  public Segment process() {
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
            _dictionaryWriterArr[i].addValue(columnEntry);
          }
          _numOfElements++;
        } else {
          logger.info("null record found");
        }
      }

      for (int i = 0; i < _columnMetadataArr.length; i++) {
        _termValueLists[i] = _dictionaryWriterArr[i].getTermValueList();
        _compressedIntArr[i] =
            new CompressedIntArray(_numOfElements, CompressedIntArray.getNumOfBits(_termValueLists[i].size()), getByteBuffer(_numOfElements, _termValueLists[i].size()));
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
          _compressedIntArr[i].addInt(incrementor, _dictionaryWriterArr[i].getValue(value));
        }
        incrementor++;
      }

      _metadataWriter =
          new MetadataWriter(_columnMetadataArr, _termValueLists, _compressedIntArr, _numOfElements);
      _forwardIndexWriter =
          new ForwardIndexWriter(_columnMetadataArr, _compressedIntArr, _termValueLists, _numOfElements);
      setUpDictionaryMap();
      setUpForwardIndexMap();
      setUpMetadataMap();
      return new Segment(_metadataMap, _termValueListMap, _compressedIntArrMap, _numOfElements);
    } catch (FileNotFoundException e) {
      logger.error(e);
    } catch (IOException e) {
      logger.error(e);
    }
    return null;
  }

  private void setUpDictionaryMap() {
    _termValueListMap = new HashMap<String, TermValueList>();
    for (int i = 0; i < _columnMetadataArr.length; i++) {
      _termValueListMap.put(_columnMetadataArr[i].getName(), _termValueLists[i]);
    }
  }

  private void setUpForwardIndexMap() {
    _compressedIntArrMap = new HashMap<String, CompressedIntArray>();
    for (int i = 0; i < _columnMetadataArr.length; i++) {
      _compressedIntArr[i].getStorage().rewind();
      _compressedIntArrMap.put(_columnMetadataArr[i].getName(), _compressedIntArr[i]);
    }
  }

  private void setUpMetadataMap() {
    _metadataMap = new HashMap<String, ColumnMedata>();
    for (int i = 0; i < _metadataWriter.getMetadataArr().length; i++) {
      _metadataMap.put(_metadataWriter.getMetadataArr()[i].getName(), _metadataWriter.getMetadataArr()[i]);
    }
  }

  public HashMap<String, ColumnMedata> getMetadataMap() {
    return _metadataMap;
  }

  public HashMap<String, TermValueList> getTermValueListMap() {
    return _termValueListMap;
  }

  public HashMap<String, CompressedIntArray> getCompressedIntMap() {
    return _compressedIntArrMap;
  }

  public void flushTo(File dir) {
    for (int i = 0; i < _columnMetadataArr.length; i++) {
      _dictionaryWriterArr[i].flush(dir.getAbsolutePath());
    }
    _forwardIndexWriter.flush(dir.getAbsolutePath());
    _metadataWriter.flush(dir.getAbsolutePath());
  }

  public ByteBuffer getByteBuffer(int numOfElements, int dictionarySize) {
    return ByteBuffer.allocate(CompressedIntArray.getRequiredBufferSize(numOfElements, CompressedIntArray.getNumOfBits(dictionarySize)));
  }

}
