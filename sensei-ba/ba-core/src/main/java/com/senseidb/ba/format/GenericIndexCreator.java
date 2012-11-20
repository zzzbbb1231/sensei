package com.senseidb.ba.format;

import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.management.RuntimeErrorException;

import org.springframework.util.Assert;

import com.browseengine.bobo.facets.data.TermLongList;
import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.MultiValueForwardIndex;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.creators.AvroSegmentCreator;
import com.senseidb.ba.gazelle.creators.ForwardIndexCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.impl.SecondarySortedForwardIndexImpl;
import com.senseidb.ba.gazelle.utils.SortUtil;

public class GenericIndexCreator {
  public static boolean canCreateSegment(String filename) {
    return filename.toLowerCase().endsWith(".json") || filename.toLowerCase().endsWith(".avro") || filename.toLowerCase().endsWith(".csv");
  }  
  public static GazelleIndexSegmentImpl create(File file) throws Exception {
    Assert.state(canCreateSegment(file.getName()));
    if (file.getName().toLowerCase().endsWith(".json")) {
      return create(new JsonDataSource(file));
    }
    if (file.getName().toLowerCase().endsWith(".csv")) {
      return create(new CSVDataSource(file));
    }
    if (file.getName().toLowerCase().endsWith(".avro")) {
      return AvroSegmentCreator.readFromAvroFile(file);
    }
    throw new UnsupportedOperationException(file.getName());
  }
  
  
  public static GazelleIndexSegmentImpl create(GazelleDataSource gazelleDataSource) throws Exception {
        try {
        Map<String, ColumnType> columnTypes = getColumnTypes(gazelleDataSource.newIterator());
        //System.out.println("!!" + gazelleDataSource.toString() + " - " + columnTypes);
        gazelleDataSource.closeCurrentIterators();
        Map<String, ForwardIndexCreator> indexCreators = new HashMap<String, ForwardIndexCreator>(columnTypes.size());
        for (String  key : columnTypes.keySet()) {
            ForwardIndexCreator indexCreator = new ForwardIndexCreator(key, columnTypes.get(key));
            indexCreators.put(key, indexCreator);
        }
        //initDictionaries
        int count = 0;
        Iterator<Map<String, Object>> iterator = gazelleDataSource.newIterator();
        while(iterator.hasNext()) {
            Map<String, Object> map = iterator.next();
            for (String  key : indexCreators.keySet()) {
                indexCreators.get(key).addValueToDictionary(map.get(key));
            }
            count++;
        }
        gazelleDataSource.closeCurrentIterators();
        for (String  key : columnTypes.keySet()) {
            indexCreators.get(key).produceDictionary(count);
        }
        iterator = gazelleDataSource.newIterator();
        
        while(iterator.hasNext()) {
            Map<String, Object> map = iterator.next();
            for (String  key : indexCreators.keySet()) {
                indexCreators.get(key).addValueToForwardIndex(map.get(key));
            }
            
        }
        gazelleDataSource.closeCurrentIterators();
        GazelleIndexSegmentImpl indexSegmentImpl = new GazelleIndexSegmentImpl();
        for (String  key : indexCreators.keySet()) {
          ForwardIndexCreator indexCreator = indexCreators.get(key);
          indexSegmentImpl.getColumnTypes().put(indexCreator.getColumnName(), indexCreator.getColumnType());
          indexSegmentImpl.getDictionaries().put(indexCreator.getColumnName(), indexCreator.getDictionary());
          
          ForwardIndex forwardIndex = indexCreator.produceForwardIndex();
         
          indexSegmentImpl.getColumnMetadataMap().put(indexCreator.getColumnName(), indexCreator.produceColumnMetadata());
          indexSegmentImpl.getForwardIndexes().put(indexCreator.getColumnName(), forwardIndex);
        }
        indexSegmentImpl.setLength(count);
        return indexSegmentImpl;
        } catch (Exception ex) {
            ex.printStackTrace();
          throw new RuntimeException(ex);
          } finally {
            gazelleDataSource.closeCurrentIterators();
          }
    }

    private static Map<String, ColumnType> getColumnTypes(Iterator<Map<String, Object>> iterator) {
        Map<String, ColumnType> columnTypes = new HashMap<String, ColumnType>();
        while(iterator.hasNext()) {
            Map<String, Object> map = iterator.next();
            for (String  key : map.keySet()) {
                ColumnType newType = ColumnType.getColumnType(map.get(key));               
                if (ColumnType.isBigger(columnTypes.get(key), newType)) {
                    columnTypes.put(key, newType);
                }
            }
        }
        return columnTypes;
    }
    public static GazelleIndexSegmentImpl create(File file, String... sortedColumns) throws Exception {
      GazelleIndexSegmentImpl nonSortedSegment = create(file);
      if (nonSortedSegment == null) {
        return null;
      }
      GazelleIndexSegmentImpl sortedSegment = create(nonSortedSegment, sortedColumns );
      return sortedSegment;
    }
    public static GazelleIndexSegmentImpl create(final GazelleIndexSegmentImpl nonSortedSegment, String[] sortedColumns) throws Exception {
      final int[] permutationArray = new int[nonSortedSegment.getLength()];
      for (int i = 0; i < permutationArray.length; i++) {
        permutationArray[i] = i;
      }
      final SingleValueForwardIndex[] forwardIndexes = new SingleValueForwardIndex[sortedColumns.length];
      for (int i = 0; i < sortedColumns.length; i++) {
        Assert.state(nonSortedSegment.getForwardIndex(sortedColumns[i]) instanceof SingleValueForwardIndex, "MultiValueColumns couldn't be sorted " + sortedColumns[i] + "");
        SingleValueForwardIndex forwardIndex = (SingleValueForwardIndex) nonSortedSegment.getForwardIndex(sortedColumns[i]);
        Assert.notNull(forwardIndex, "Index for the column " + sortedColumns[i] + " doesn't exist");
        forwardIndexes[i] = forwardIndex;
      }
      SortUtil.quickSort(0, permutationArray.length, new SortUtil.IntComparator() {
        @Override
        public int compare(Integer o1, Integer o2) {
          return compare(o1.intValue(), o2.intValue());
        }
        @Override
        public int compare(int k1, int k2) {
          for (SingleValueForwardIndex index :  forwardIndexes) {
            int val1 = index.getValueIndex(permutationArray[k1]);
            int val2 = index.getValueIndex(permutationArray[k2]);
            if (val1 > val2) return 1;
            if (val2 > val1) return -1;
           
          }
          return 0; 
        }
      }, new SortUtil.Swapper() {
        @Override
        public void swap(int a, int b) {
          int tmp = permutationArray[b];
          permutationArray[b] = permutationArray[a];
          permutationArray[a] = tmp;
        }
      });
      return  create(new AbstractDataSource() {
        @Override
        public AbstractGazelleIterator createIterator() {
          return new SortedSegmentIterator(nonSortedSegment, permutationArray);
        }
      });  
    }
    
    public static class SortedSegmentIterator extends  AbstractGazelleIterator {
      private final GazelleIndexSegmentImpl nonSortedSegment;
      private final int[] permutationArray;
      private int counter;
      private final int[] buffer;

      public SortedSegmentIterator(GazelleIndexSegmentImpl nonSortedSegment, int[] permutationArray) {
        this.nonSortedSegment = nonSortedSegment;
        this.permutationArray = permutationArray;
         counter = 0;
         int bufferSize = 0;
         for (ForwardIndex forwardIndex : nonSortedSegment.getForwardIndexes().values()) {
           if (forwardIndex instanceof MultiValueForwardIndex) {
              int maxNumValuesPerDoc = ((MultiValueForwardIndex)forwardIndex).getMaxNumValuesPerDoc();
              if (maxNumValuesPerDoc > bufferSize) {
                bufferSize = maxNumValuesPerDoc;
              }
           }
         }
         buffer = new int[bufferSize];
      }
      @Override
      public boolean hasNext() {
        return counter < nonSortedSegment.getLength();
      }

      @Override
      public Map<String, Object> next() {
        Map<String, Object> ret = new HashMap<String, Object>();
        for (String column : nonSortedSegment.getColumnTypes().keySet()) {
          int docId = permutationArray[counter];
          ForwardIndex forwardIndex = nonSortedSegment.getForwardIndex(column);
          if (forwardIndex instanceof SingleValueForwardIndex) {              
            int valueIndex = ((SingleValueForwardIndex) forwardIndex).getValueIndex(docId);
             if (valueIndex < 0) {
               System.out.println("!!!");
             } 
             ret.put(column, forwardIndex.getDictionary().getRawValue(valueIndex));
          } else {
            MultiValueForwardIndex multiValueForwardIndex = (MultiValueForwardIndex)  forwardIndex;
            int count = multiValueForwardIndex.randomRead(buffer, docId);
            Object[] value = new Object[count];
            for (int i = 0; i < count; i ++) {
              value[i] = forwardIndex.getDictionary().getRawValue(buffer[i]);
            }
            ret.put(column, value);
          }
        }
        counter++;
        return ret;
      }

      @Override
      public void close() {
      }
      
    }
}
