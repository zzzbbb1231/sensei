package com.senseidb.ba.gazelle.creators.prototype;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.springframework.util.Assert;

import com.browseengine.bobo.facets.data.TermFloatList;
import com.browseengine.bobo.facets.data.TermIntList;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.MetadataAware;
import com.senseidb.ba.gazelle.creators.ForwardIndexCreator;
import com.senseidb.ba.gazelle.creators.MetadataCreator;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.impl.SortedForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.CompressedIntArray;
import com.senseidb.ba.gazelle.utils.multi.CompressedMultiArray;

public class AvroSinglePassCreator {
  private final int length;
  private  String[] sortedFields;
  private SinglePassIndexCreator[] creators;
  private int currentRowCount = 0;
  private int[] sortedFieldIndexes;
  private List<Field> fields;
  public AvroSinglePassCreator(int length, String[] sortedFields) {
    this.length = length;
    this.sortedFields = sortedFields;    
  }
  public void addRecord(Record record) {
    if (creators == null) {
      initSchema(record);
    }
    for (int i = 0; i < creators.length; i++) {
      Object value = record.get(i);
      if (value instanceof Utf8) {
        value = value.toString();
      }
      if (value instanceof Array) {
        value = ForwardIndexCreator.transform((Array) value);
      }
      creators[i].addValue(currentRowCount, value);
    }
    currentRowCount++;
  }
  public void initSchema(Record record) {
     fields = record.getSchema().getFields();
    creators = new SinglePassIndexCreator[fields.size()];
    for (int i = 0; i < creators.length; i++) {
      creators[i] = new SinglePassIndexCreator(length);        
    }
    sortedFieldIndexes = new int[sortedFields.length];
    for (int i = 0; i < sortedFieldIndexes.length; i++) {
      boolean found = false;
      for (int j = 0; j < fields.size(); j++) {
        if (sortedFields[i].equals(fields.get(j).name())) {
          sortedFieldIndexes[i] = j;
          found = true;
          break;
        }
      }
      Assert.state(found);
    }
  }
  public GazelleIndexSegmentImpl produceSegment() throws Exception {
    TermValueList[] dictionaries = new TermValueList[creators.length];
    ForwardIndex[] forwardIndexes = new ForwardIndex[creators.length];
    ColumnMetadata[] columnMetadatas = new ColumnMetadata[creators.length];
    for (int i = 0; i < dictionaries.length; i++) {
      dictionaries[i] = creators[i].seal();
    }
    boolean[] isSorted = new boolean[dictionaries.length];
    int[] permutationArray = null;
    if (sortedFieldIndexes.length > 0) {
      final int[] permutationArrayCopy = new int[currentRowCount + 1];
      permutationArray = permutationArrayCopy;
      it.unimi.dsi.fastutil.Arrays.quickSort(0, permutationArray.length, new IntComparator() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return compare(o1.intValue(), o2.intValue());
        }
        @Override
        public int compare(int k1, int k2) {
          for (int index : sortedFieldIndexes) {
            int val1 = creators[permutationArrayCopy[index]].getValueIndex(k1);
            int val2 = creators[permutationArrayCopy[index]].getValueIndex(k2);
            if (val1 > val2) return 1;
            if (val2 > val1) return -1;
           
          }
          return 0; 
        }
      }, new Swapper() {
        @Override
        public void swap(int a, int b) {
          int tmp = permutationArrayCopy[b];
          permutationArrayCopy[b] = permutationArrayCopy[a];
          permutationArrayCopy[a] = tmp;
        }
      });
      for (int i = 0; i < creators.length; i++) {
        SinglePassIndexCreator creator = creators[i];
        if (creator.isMulti()) {
          continue;
        }
        isSorted[i] = true;
        for (int index = 0; index < currentRowCount; i++) {
          if (creator.getValueIndex(permutationArray[index]) > creator.getValueIndex(permutationArray[index + 1])) {
            isSorted[i] = false;
            break;
          }
        }
      }
    } else {
      for (int i = 0; i < creators.length; i++) {
        SinglePassIndexCreator creator = creators[i];
        if (creator.isMulti()) {
          continue;
        }
        isSorted[i] = true;
        for (int index = 0; index < currentRowCount; i++) {
          if (creator.getValueIndex(index) > creator.getValueIndex(index + 1)) {
            isSorted[i] = false;
            break;
          }
        }
      }
    }
    GazelleIndexSegmentImpl gazelleIndexSegmentImpl = new GazelleIndexSegmentImpl();
    gazelleIndexSegmentImpl.setLength(currentRowCount + 1);
    for (int i = 0; i < creators.length; i++) {
      String columnName = fields.get(i).name();
      ForwardIndex forwardIndex = createForwardIndex(creators[i], columnName, isSorted[i], dictionaries[i], permutationArray, currentRowCount + 1);
      gazelleIndexSegmentImpl.getForwardIndexes().put(columnName, forwardIndex);
      gazelleIndexSegmentImpl.getDictionaries().put(columnName, dictionaries[i]);
      gazelleIndexSegmentImpl.getColumnMetadataMap().put(columnName, ((MetadataAware)forwardIndex).getColumnMetadata());
    }
    return gazelleIndexSegmentImpl;
  }
  private ForwardIndex createForwardIndex(SinglePassIndexCreator singlePassIndexCreator, String columnName, boolean isSorted, TermValueList termValueList, int[] permutationArray, int length) {
   if (singlePassIndexCreator.isMulti()) {
     CompressedMultiArray compressedMultiArray = new CompressedMultiArray(CompressedIntArray.getNumOfBits(termValueList.size()), length);
     int index;
     int[] buffer = new int[singlePassIndexCreator.getMaxValuesCount()];
     for (int i = 0; i < length; i++) {
       if (permutationArray == null) {
         index = i;
       } else {
         index = permutationArray[i];
       }
       int count = singlePassIndexCreator.getValueIndexes(i, buffer);
       compressedMultiArray.add(buffer, count);
     }
     compressedMultiArray.initSkipLists();
     return new com.senseidb.ba.gazelle.impl.MultiValueForwardIndexImpl1(columnName, compressedMultiArray, termValueList, MetadataCreator.createMultiMetadata(columnName, termValueList, getType(termValueList), length));
   }
   if (!isSorted) {
     CompressedIntArray compressedIntArray = new CompressedIntArray(CompressedIntArray.getNumOfBits(termValueList.size()), length);
     int index;    
     for (int i = 0; i < length; i++) {
       if (permutationArray == null) {
         index = i;
       } else {
         index = permutationArray[i];
       }
       compressedIntArray.addInt(i, singlePassIndexCreator.getValueIndex(index));
     }
    
     return new GazelleForwardIndexImpl(columnName, compressedIntArray, termValueList, MetadataCreator.createMetadata(columnName, termValueList, getType(termValueList), length, false));
   } else {
     SortedForwardIndexImpl sortedForwardIndexImpl = new SortedForwardIndexImpl(termValueList, new int[termValueList.size()], new int[termValueList.size()], length, MetadataCreator.createMetadata(columnName, termValueList, getType(termValueList), length, true));
     int index;    
     for (int i = 0; i < length; i++) {
       if (permutationArray == null) {
         index = i;
       } else {
         index = permutationArray[i];
       }
       sortedForwardIndexImpl.add(i, singlePassIndexCreator.getValueIndex(index));
     }
     sortedForwardIndexImpl.seal();
     return sortedForwardIndexImpl;
   }
   
  }
  private ColumnType getType(TermValueList termValueList) {
    if (termValueList instanceof TermStringList) {
      return ColumnType.STRING;
    }
    if (termValueList instanceof TermIntList) {
      return ColumnType.INT;
    }
    if (termValueList instanceof TermLongList) {
      return ColumnType.LONG;
    }
    if (termValueList instanceof TermFloatList) {
      return ColumnType.FLOAT;
    }
    throw new UnsupportedOperationException(termValueList.getClass().toString());
  }
  
}
