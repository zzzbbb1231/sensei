package com.senseidb.ba.realtime.indexing.providers;

import com.senseidb.ba.realtime.Schema;

public interface RealtimeDataTransformer<T> {
  public Object[] transform(T rawData, Schema schema);
}
