package com.senseidb.ba.realtime.domain.primitives.dictionaries;

import java.util.concurrent.locks.ReadWriteLock;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.domain.AbstractDictionarySnapshot;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;
import com.senseidb.ba.realtime.domain.SingleValueSearchSnapshot;
import com.senseidb.ba.realtime.domain.primitives.dictionaries.StringDictionarySnapshot;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class StringRealtimeDictionary implements RealtimeDictionary {  
    private Object2IntOpenHashMap<String> dictionary;
  
    public void init() {
     
      dictionary = new Object2IntOpenHashMap<String>(500);
     
    }
    
    public int addString(String value, ReadWriteLock lock) {
      int dictionaryId;
      if (dictionary.containsKey(value)) {
        dictionaryId = dictionary.get(value);
      } else {
        try {
          lock.writeLock().lock();
          // we leave 0 as reserved value. Because of the possible concurrency issues, this would mean that we hit unitialized field
          //another value is reserved for null
          dictionaryId  = dictionary.size() + 2;
          dictionary.put(value, dictionaryId);
        } finally {
          lock.writeLock().unlock();
        }
      }  
      return dictionaryId;
    }

 
    @Override
    public int addElement(Object value, ReadWriteLock readWriteLock) {
      if (value == null) {
        return NULL_DICTIONARY_ID;
       
      } else {
        return addString(value.toString(), readWriteLock);
      }
      
    }
    public DictionarySnapshot produceDictSnapshot(ReadWriteLock readWriteLock, ReusableIndexObjectsPool indexObjectsPool, String column) {  

      try {
        readWriteLock.readLock().lock();
        StringDictionarySnapshot dictionarySnapshot = null;
        AbstractDictionarySnapshot snapshotFromPool = (AbstractDictionarySnapshot) indexObjectsPool.getDictSnapshot(column);
        if (snapshotFromPool != null) {
          dictionarySnapshot = (StringDictionarySnapshot) snapshotFromPool;
        } else {
          dictionarySnapshot =  new StringDictionarySnapshot();
        }  
         
          dictionarySnapshot.init(dictionary, readWriteLock);
         return dictionarySnapshot;
        
      
      } finally {
        readWriteLock.readLock().unlock();
      }
     
    }
    @Override
    public int size() {
      return dictionary.size() + 2;
    } 
    @Override
    public void recycle() {
      dictionary.clear();
      
    } 
}
