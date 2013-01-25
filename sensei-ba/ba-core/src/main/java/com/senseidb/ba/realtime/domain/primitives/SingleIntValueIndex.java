package com.senseidb.ba.realtime.domain.primitives;

import java.util.concurrent.locks.ReadWriteLock;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.SingleValueSearchSnapshot;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

public class SingleIntValueIndex extends  AbstractFieldRealtimeIndex {
    private Int2IntOpenHashMap dictionary;
    
    
    public SingleIntValueIndex(int capacity) {
     super(capacity);
      dictionary = new Int2IntOpenHashMap(500);
      //unitialized value
      dictionary.put(Integer.MIN_VALUE, 0);
      //null value
      dictionary.put(Integer.MIN_VALUE + 1, 1);
    }
    
    public void addInt(int value, ReadWriteLock lock) {
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
        addInt(((Number) value).intValue(), readWriteLock);
      } else if (value instanceof String) {
        addInt(Integer.parseInt(value.toString()), readWriteLock);
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
          IntDictionarySnapshot dictionarySnapshot = new IntDictionarySnapshot();
          dictionarySnapshot.init(dictionary, readWriteLock);
          searchSnapshot.init(forwardIndex, currentPosition, ColumnType.INT, dictionarySnapshot);
        
        }
      
      } finally {
        readWriteLock.readLock().unlock();
      }
     
      return searchSnapshot;
    }
    
}
