package com.senseidb.ba.format;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.springframework.util.Assert;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.creators.AvroSegmentCreator;
import com.senseidb.ba.gazelle.creators.ForwardIndexCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;

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
          if (key.contains("skil")) {
            System.out.println("");
          }
          indexSegmentImpl.getColumnMetadataMap().put(indexCreator.getColumnName(), indexCreator.produceColumnMetadata());
          indexSegmentImpl.getForwardIndexes().put(indexCreator.getColumnName(), forwardIndex);
        }
        indexSegmentImpl.setLength(count);
        return indexSegmentImpl;
        } catch (Exception ex) {
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
}
