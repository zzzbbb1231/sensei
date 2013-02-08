package com.senseidb.ba.realtime.domain.primitives.dictionaries;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.util.concurrent.locks.ReadWriteLock;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.domain.AbstractDictionarySnapshot;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;
import com.senseidb.ba.realtime.domain.SingleValueSearchSnapshot;

public class LongRealtimeDictionary implements RealtimeDictionary {
  private Long2IntOpenHashMap dictionary;
  
  
  public void init() {
  
    dictionary = new Long2IntOpenHashMap(500);
    //unitialized value
    dictionary.put(Long.MIN_VALUE, 0);
    //null value
    dictionary.put(Long.MIN_VALUE + 1, 1);
  }
  
  public int addLong(long value, ReadWriteLock lock) {
    int dictionaryId;
    if (dictionary.containsKey(value)) {
      dictionaryId = dictionary.get(value);
    } else {
      try {
        lock.writeLock().lock();
        // we leave 0 as reserved value. Because of the possible concurrency issues, this would mean that we hit unitialized field
        //another value is reserved for null
        dictionaryId = dictionary.size();
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
    } else if (value instanceof Number) {
      return addLong(((Number) value).longValue(), readWriteLock);
    } else if (value instanceof String) {
      return addLong(Long.parseLong(value.toString()), readWriteLock);
    } else {
      throw new UnsupportedOperationException(value.getClass().toString());
    }
  }
  public DictionarySnapshot produceDictSnapshot(ReadWriteLock readWriteLock, ReusableIndexObjectsPool indexObjectsPool, String column) {     
   
    try {
      readWriteLock.readLock().lock();
      LongDictionarySnapshot dictionarySnapshot = null;
      DictionarySnapshot snapshotFromPool = indexObjectsPool.getDictSnapshot(column);
      if (snapshotFromPool != null) {
        dictionarySnapshot = (LongDictionarySnapshot) snapshotFromPool;
      } else {
        dictionarySnapshot =  new LongDictionarySnapshot(indexObjectsPool, column);
      }  
        dictionarySnapshot.init(dictionary, readWriteLock);
        return dictionarySnapshot;
      
      }
    
     finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public int size() {
    return dictionary.size();
  } 
  @Override
  public void recycle() {
    dictionary.clear();
    dictionary.put(Long.MIN_VALUE, 0);
    //null value
    dictionary.put(Long.MIN_VALUE + 1, 1);
  } 

}
