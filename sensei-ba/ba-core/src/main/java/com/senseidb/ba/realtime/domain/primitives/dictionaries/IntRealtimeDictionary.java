package com.senseidb.ba.realtime.domain.primitives.dictionaries;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.concurrent.locks.ReadWriteLock;

import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.domain.AbstractDictionarySnapshot;
import com.senseidb.ba.realtime.domain.DictionarySnapshot;

public class IntRealtimeDictionary implements RealtimeDictionary {
  private Int2IntOpenHashMap dictionary;
  
  
  public void init() {
  
    dictionary = new Int2IntOpenHashMap(500);
    //unitialized value
    dictionary.put(Integer.MIN_VALUE, 0);
    //null value
    dictionary.put(Integer.MIN_VALUE + 1, 1);
  }
  
  public int addInteger(int value, ReadWriteLock lock) {
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
      return addInteger(((Number) value).intValue(), readWriteLock);
    } else if (value instanceof String) {
      return addInteger(Integer.parseInt(value.toString()), readWriteLock);
    } else {
      throw new UnsupportedOperationException(value.getClass().toString());
    }
  }
  public DictionarySnapshot produceDictSnapshot(ReadWriteLock readWriteLock, ReusableIndexObjectsPool indexObjectsPool, String column) {     
   
    try {
      readWriteLock.readLock().lock();
      IntDictionarySnapshot dictionarySnapshot = null;
      DictionarySnapshot snapshotFromPool = (DictionarySnapshot) indexObjectsPool.getDictSnapshot(column);
      if (snapshotFromPool != null) {
        dictionarySnapshot = (IntDictionarySnapshot) snapshotFromPool;
      } else {
        dictionarySnapshot =  new IntDictionarySnapshot();
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

}
