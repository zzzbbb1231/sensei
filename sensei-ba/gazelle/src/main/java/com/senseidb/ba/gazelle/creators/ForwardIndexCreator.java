package com.senseidb.ba.gazelle.creators;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.springframework.util.Assert;

import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.MultiValueForwardIndexImpl1;
import com.senseidb.ba.gazelle.impl.SortedForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.gazelle.utils.multi.CompressedMultiArray;

public class ForwardIndexCreator {
    private final String columnName;
    private final ColumnType columnType;
    private DictionaryCreator dictionaryCreator;
    private TermValueList<?> dictionary;
    private int count;
    private CompressedIntArray compressedIntArray;
    private SortedForwardIndexImpl sortedForwardIndexImpl;
    private CompressedMultiArray compressedMultiArray;
    private int i = 0;
    private ColumnMetadata metadata;
    public ForwardIndexCreator(String columnName, ColumnType columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
        dictionaryCreator = new DictionaryCreator();
    }
    
    public void addValueToDictionary(Object value) {
        if (value instanceof Utf8) {
            value = ((Utf8) value).toString();
          } else if (value  instanceof Array) {
              value = transform((Array) value);
          }
        dictionaryCreator.addValue(value, columnType);
    }
    @SuppressWarnings("rawtypes")
    public TermValueList produceDictionary(int count) {
        this.count = count;
        dictionary = dictionaryCreator.produceDictionary();
        if (!dictionaryCreator.isSorted() && !columnType.isMulti()) {
            compressedIntArray = new CompressedIntArray(count, CompressedIntArray.getNumOfBits(dictionary.size()), getByteBuffer(count, dictionary.size()));
        } else if (dictionaryCreator.isSorted()) {
            sortedForwardIndexImpl = new SortedForwardIndexImpl(dictionary, new int[dictionary.size()], new int[dictionary.size()], count, MetadataCreator.createMetadata(columnName, dictionary, columnType, count, true));
        } else if (columnType.isMulti()) {
            compressedMultiArray = new CompressedMultiArray(CompressedIntArray.getNumOfBits(dictionary.size()), count * 2);
        }
        return dictionary;
    }
    public void addValueToForwardIndex(Object value) {
        if (compressedIntArray != null) {
            compressedIntArray.addInt(i, dictionaryCreator.getIndex(value, columnType));
            i++;
        }
        if (sortedForwardIndexImpl != null) {
            sortedForwardIndexImpl.add(i, dictionaryCreator.getIndex(value, columnType));
            i++;
        }
        if (compressedMultiArray != null) {
            compressedMultiArray.add(dictionaryCreator.getIndexes(transform((Array) value), columnType));
        }
    }
    
    public static Object[] transform(GenericData.Array arr) {
        if (arr == null) {
            return new Object[0];
        }
        Object[] ret = new Object[(int)arr.size()];
        Iterator iterator = arr.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            Object value = iterator.next();
            if (value instanceof Record) {
              value = ((Record)value).get(0);
            }
            if (value instanceof Utf8) {
                value = ((Utf8) value).toString();
              }
            ret[i++] = value;
        }
        Assert.state(i == ret.length);
        return ret;
    }
    public ForwardIndex produceForwardIndex() {
        if (compressedIntArray != null) {
             metadata = MetadataCreator.createMetadata(columnName, dictionary, columnType, count, false);
            return new GazelleForwardIndexImpl(columnName, compressedIntArray, dictionary, metadata);
        }
        if (sortedForwardIndexImpl != null) {
            metadata = sortedForwardIndexImpl.getColumnMetadata();
            sortedForwardIndexImpl.seal();
            return sortedForwardIndexImpl;
        }
        if (compressedMultiArray != null) {
            compressedMultiArray.initSkipLists();
            metadata = MetadataCreator.createMultiMetadata(columnName, dictionary, columnType, count);
            return new MultiValueForwardIndexImpl1(columnName, compressedMultiArray, dictionary, metadata);
        }
        throw new UnsupportedOperationException();
    }
   public ColumnMetadata produceColumnMetadata() {
        return metadata;
    }

public String getColumnName() {
    return columnName;
}

public ColumnType getColumnType() {
    return columnType;
}



public TermValueList<?> getDictionary() {
    return dictionary;
}

private static ByteBuffer getByteBuffer(int numOfElements, int dictionarySize) {
    return ByteBuffer.allocate((int) CompressedIntArray.getRequiredBufferSize(numOfElements, CompressedIntArray.getNumOfBits(dictionarySize)));
  }
    
}
