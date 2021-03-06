package com.senseidb.ba.realtime;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.springframework.util.Assert;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.domain.ColumnSearchSnapshot;
import com.senseidb.ba.realtime.domain.RealtimeSnapshotIndexSegment;
import com.senseidb.ba.realtime.domain.primitives.FieldRealtimeIndex;
import com.senseidb.ba.realtime.domain.primitives.MultiFieldRealtimeIndex;
import com.senseidb.ba.realtime.domain.primitives.SingleFieldRealtimeIndex;
import com.senseidb.ba.realtime.domain.primitives.dictionaries.FloatRealtimeDictionary;
import com.senseidb.ba.realtime.domain.primitives.dictionaries.IntRealtimeDictionary;
import com.senseidb.ba.realtime.domain.primitives.dictionaries.LongRealtimeDictionary;
import com.senseidb.ba.realtime.domain.primitives.dictionaries.RealtimeDictionary;
import com.senseidb.ba.realtime.domain.primitives.dictionaries.StringRealtimeDictionary;

public class SegmentAppendableIndex {
    private int capacity;
    private volatile int currenIndex = 0;
    private FieldRealtimeIndex[] columnIndexes;
    private Schema schema; 
    volatile private  String version;
    private String name;
    private long indexingStartTime;
    private long indexingEndTime;
    
    private SegmentResurrectingMarker segmentResurrectingMarker;
    public SegmentAppendableIndex(ReusableIndexObjectsPool indexObjectsPool) {
       segmentResurrectingMarker = new SegmentResurrectingMarker(indexObjectsPool, this);
    }
    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    public void init(Schema schema, int capacity) {
      this.schema = schema;
      this.capacity = capacity;
      columnIndexes = new FieldRealtimeIndex[schema.getColumnNames().length];
      for (int i = 0; i < schema.getColumnNames().length; i++) {
        ColumnType columnType = schema.getTypes()[i];
        RealtimeDictionary dictionary = null;
        boolean isSingleValue = !columnType.isMulti();
        if (columnType.getElementType() == ColumnType.LONG) {
          dictionary = new LongRealtimeDictionary();
        } else if (columnType.getElementType() == ColumnType.STRING) {
          dictionary = new StringRealtimeDictionary();
        } else if (columnType.getElementType() == ColumnType.FLOAT) {
          dictionary = new FloatRealtimeDictionary();
        } else if (columnType.getElementType() == ColumnType.INT) {
          dictionary = new IntRealtimeDictionary();
        } else {
          throw new UnsupportedOperationException(columnType.toString());
        }
        
        dictionary.init();
        if (isSingleValue) {
          columnIndexes[i] = new SingleFieldRealtimeIndex(dictionary, columnType, capacity);
        } else {
          columnIndexes[i] = new MultiFieldRealtimeIndex(dictionary, columnType, capacity);
        }
      }
    }
    public boolean add(Object[] values, String version) {
      Assert.state(values.length == columnIndexes.length);
      try {
        //readWriteLock.readLock().lock();
        for (int i = 0; i < schema.getColumnNames().length; i++) {
          columnIndexes[i].addElement(values[i], readWriteLock);
        }
      } finally {
        //readWriteLock.readLock().unlock();
      }
      this.version = version;
      currenIndex++;
      return currenIndex == capacity;
      }
    private RealtimeSnapshotIndexSegment previousSnapshot = null; 
    public synchronized RealtimeSnapshotIndexSegment refreshSearchSnapshot(ReusableIndexObjectsPool reusableIndexObjectsPool) {
      int indexToSyncOn = currenIndex;
      if (previousSnapshot != null && previousSnapshot.getLength() == indexToSyncOn) {
        boolean stillNeedToRefresh = false;
        for (int i = 0; i < schema.getColumnNames().length; i++) {
          if (previousSnapshot.getForwardIndex(schema.getColumnNames()[i]).getForwardIndexSize() < indexToSyncOn) {
            stillNeedToRefresh = true;
            break;
          }
        }
        if (!stillNeedToRefresh) {
          return previousSnapshot;
        }
      }
      Map<String, ColumnSearchSnapshot> columnSnapshots = new HashMap<String, ColumnSearchSnapshot>();
      Map<String, ColumnType> columnTypes = new HashMap<String, ColumnType>();
      for (int i = 0; i < schema.getColumnNames().length; i++) {
        String column = schema.getColumnNames()[i];
        columnTypes.put(column, schema.getTypes()[i]);
        ColumnSearchSnapshot newSnapshot = columnIndexes[i].produceSnapshot(readWriteLock, reusableIndexObjectsPool,  column);
        if (newSnapshot.getForwardIndexSize() > indexToSyncOn) {
          newSnapshot.setForwardIndexSize(indexToSyncOn);
        } 
        newSnapshot.setForwardIndexSize(indexToSyncOn);
        columnSnapshots.put(column, newSnapshot);
      
      }
      previousSnapshot = new RealtimeSnapshotIndexSegment(indexToSyncOn, columnSnapshots, columnTypes);
      previousSnapshot.setReferencedSegment(this);
      return previousSnapshot;

    }
    public FieldRealtimeIndex[] getColumnIndexes() {
      return columnIndexes;
    }
    public int getCurrenIndex() {
      return currenIndex;
    }
    public void recycle() {
      version = null;
      name = null;
      currenIndex = 0;
     
      for (FieldRealtimeIndex index :   this.getColumnIndexes()) {
        index.recycle();
      }
    }
    public String getVersion() {
      return version;
    }
    public String getName() {
      return name;
    }
    public void setVersion(String version) {
      this.version = version;
    }
    public void setName(String name) {
      this.name = name;
    }
    
    public long getIndexingStartTime() {
      return indexingStartTime;
    }
    public long getIndexingEndTime() {
      return indexingEndTime;
    }
    public void setIndexingStartTime(long indexingStartTime) {
      this.indexingStartTime = indexingStartTime;
    }
    public void setIndexingEndTime(long indexingEndTime) {
      this.indexingEndTime = indexingEndTime;
    }
    public SegmentResurrectingMarker getSegmentResurrectingMarker() {
      return segmentResurrectingMarker;
    }
   
    
}
