package com.senseidb.ba.realtime;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.springframework.util.Assert;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.primitives.FieldRealtimeIndex;
import com.senseidb.ba.realtime.primitives.SingleLongValueIndex;
import com.senseidb.ba.realtime.primitives.SingleStringValueIndex;

public class SegmentAppendableIndex {
    private int capacity;
    private volatile int currenIndex = 0;;
    private FieldRealtimeIndex[] columnIndexes;
    private Schema schema; 
    volatile private  String version;
     
    
    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    public void init(Schema schema, int capacity) {
      this.schema = schema;
      this.capacity = capacity;
      columnIndexes = new FieldRealtimeIndex[schema.getColumnNames().length];
      for (int i = 0; i < schema.getColumnNames().length; i++) {
        String column = schema.getColumnNames()[i];
        ColumnType columnType = schema.getTypes()[i];
        if (columnType == ColumnType.LONG) {
          columnIndexes[i] = new SingleLongValueIndex(capacity);
        } else if (columnType == ColumnType.STRING) {
          columnIndexes[i] = new SingleStringValueIndex(capacity);
        } else {
          throw new UnsupportedOperationException(columnType.toString());
        }
      }
    }
    public boolean add(Object[] values, String version) {
      Assert.state(values.length == columnIndexes.length);
      try {
        readWriteLock.readLock().lock();
        for (int i = 0; i < schema.getColumnNames().length; i++) {
          columnIndexes[i].addElement(values[i], readWriteLock);
        }
      } finally {
        readWriteLock.readLock().unlock();
      }
      this.version = version;
      currenIndex++;
      return currenIndex == capacity;
      }
    private RealtimeSnapshotIndexSegment previousSnapshot = null; 
    public synchronized RealtimeSnapshotIndexSegment getSearchSnapshot() {
      if (previousSnapshot != null && previousSnapshot.getLength() == currenIndex) {
        return previousSnapshot;
      }
      Map<String, ColumnSearchSnapshot> columnSnapshots = new HashMap<String, ColumnSearchSnapshot>();
      Map<String, ColumnType> columnTypes = new HashMap<String, ColumnType>();
      for (int i = 0; i < schema.getColumnNames().length; i++) {
        String column = schema.getColumnNames()[i];
        columnTypes.put(column, schema.getTypes()[i]);
        columnSnapshots.put(column, columnIndexes[i].produceSnapshot(readWriteLock));
      }
      previousSnapshot = new RealtimeSnapshotIndexSegment(currenIndex, columnSnapshots, columnTypes);
      return previousSnapshot;

    }
    public FieldRealtimeIndex[] getColumnIndexes() {
      return columnIndexes;
    }
    public int getCurrenIndex() {
      return currenIndex;
    }
  
    
    
}
