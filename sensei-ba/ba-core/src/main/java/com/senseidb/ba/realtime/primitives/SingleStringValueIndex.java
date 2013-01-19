package com.senseidb.ba.realtime.primitives;

import java.util.concurrent.locks.ReadWriteLock;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.ColumnSearchSnapshot;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class SingleStringValueIndex extends AbstractFieldRealtimeIndex  {
    private Object2IntOpenHashMap<String> dictionary;
  
    public SingleStringValueIndex(int capacity) {
      super(capacity);
      dictionary = new Object2IntOpenHashMap<String>(500);
      //unitialized value
      dictionary.put("", 0);
      //null value
      dictionary.put("", 1);
    }
    
    public void addString(String value, ReadWriteLock lock) {
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
      forwardIndex[currentPosition] = dictionaryId;
      currentPosition++;
    }

 
    @Override
    public void addElement(Object value, ReadWriteLock readWriteLock) {
      if (value == null) {
        forwardIndex[currentPosition] = NULL_DICTIONARY_ID;
        currentPosition++;
      }
        addString(value.toString(), readWriteLock);
      
    }
    public ColumnSearchSnapshot produceSnapshot(ReadWriteLock readWriteLock) {     

      try {
        readWriteLock.readLock().lock();
        if (searchSnapshot != null && searchSnapshot.getPermutationArray() != null && searchSnapshot.getPermutationArray().size() == dictionary.size()) {
         
        } else {
          searchSnapshot = new SingleStringValueSearchSnapshot();
          ((SingleStringValueSearchSnapshot)searchSnapshot).init(dictionary, readWriteLock);
        }
      
      } finally {
        readWriteLock.readLock().unlock();
      }
      searchSnapshot.initForwardIndex(forwardIndex, currentPosition, ColumnType.STRING);
      return searchSnapshot;
    }
}
