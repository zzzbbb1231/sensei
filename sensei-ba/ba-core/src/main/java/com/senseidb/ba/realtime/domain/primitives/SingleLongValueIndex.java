package com.senseidb.ba.realtime.domain.primitives;

import java.util.concurrent.locks.ReadWriteLock;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.SingleValueSearchSnapshot;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

public class SingleLongValueIndex extends  AbstractFieldRealtimeIndex {
    private Long2IntOpenHashMap dictionary;
    
    
    public SingleLongValueIndex(int capacity) {
     super(capacity);
      dictionary = new Long2IntOpenHashMap(500);
      //unitialized value
      dictionary.put(Long.MIN_VALUE, 0);
      //null value
      dictionary.put(Long.MIN_VALUE + 1, 1);
    }
    
    public void addLong(long value, ReadWriteLock lock) {
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
      } else if (value instanceof Number) {
        addLong(((Number) value).longValue(), readWriteLock);
      } else if (value instanceof String) {
        addLong(Long.parseLong(value.toString()), readWriteLock);
      } else {
        throw new UnsupportedOperationException(value.getClass().toString());
      }
    }
    public ColumnSearchSnapshot produceSnapshot(ReadWriteLock readWriteLock) {     
     
      try {
        readWriteLock.readLock().lock();
        if (searchSnapshot != null && searchSnapshot.getDictionarySnapshot().getDictPermutationArray() != null && searchSnapshot.getDictionarySnapshot().getDictPermutationArray().size() == dictionary.size()) {
         
        } else {
          
          searchSnapshot = new SingleValueSearchSnapshot();
          LongDictionarySnapshot dictionarySnapshot = new LongDictionarySnapshot();
          dictionarySnapshot.init(dictionary, readWriteLock);
          searchSnapshot.init(forwardIndex, currentPosition, ColumnType.LONG, dictionarySnapshot);
        
        }
      
      } finally {
        readWriteLock.readLock().unlock();
      }
     
      return searchSnapshot;
    }
   
}
