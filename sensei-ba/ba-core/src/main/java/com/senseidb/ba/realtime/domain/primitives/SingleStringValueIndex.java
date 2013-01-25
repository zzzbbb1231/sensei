package com.senseidb.ba.realtime.domain.primitives;

import java.util.concurrent.locks.ReadWriteLock;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.SingleValueSearchSnapshot;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

public class SingleStringValueIndex extends AbstractFieldRealtimeIndex  {
    private Object2IntOpenHashMap<String> dictionary;
  
    public SingleStringValueIndex(int capacity) {
      super(capacity);
      dictionary = new Object2IntOpenHashMap<String>(500);
     
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
          dictionaryId  = dictionary.size() + 2;
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
      } else {
        addString(value.toString(), readWriteLock);
      }
      
    }
    public ColumnSearchSnapshot produceSnapshot(ReadWriteLock readWriteLock) {     

      try {
        readWriteLock.readLock().lock();
        if (searchSnapshot != null && searchSnapshot.getDictionarySnapshot().getDictPermutationArray() != null && searchSnapshot.getDictionarySnapshot().getDictPermutationArray().size() == dictionary.size()) {
         
        } else {
          searchSnapshot = new SingleValueSearchSnapshot();
          StringDictionarySnapshot dictionarySnapshot = new StringDictionarySnapshot();
          dictionarySnapshot.init(dictionary, readWriteLock);
          searchSnapshot.init(forwardIndex, currentPosition, ColumnType.STRING, dictionarySnapshot);
        }
      
      } finally {
        readWriteLock.readLock().unlock();
      }
      return searchSnapshot;
    }
}
