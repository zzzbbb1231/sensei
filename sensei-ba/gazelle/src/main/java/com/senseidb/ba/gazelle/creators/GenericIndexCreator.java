package com.senseidb.ba.gazelle.creators;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.ForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;

public class GenericIndexCreator {
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
          System.out.println(key);
          ForwardIndex forwardIndex = indexCreator.produceForwardIndex();
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
