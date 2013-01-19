package com.senseidb.ba.realtime.indexing;

import java.util.Map;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.Schema;

public interface RealtimeDataProvider {
    public void init(String lastVersion);
    public Schema getSchema();
    public DataWithVersion next();
    public void commit(String version);
}
