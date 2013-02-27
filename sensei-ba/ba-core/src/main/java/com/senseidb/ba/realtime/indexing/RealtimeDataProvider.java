package com.senseidb.ba.realtime.indexing;

import java.util.Map;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.realtime.Schema;

public interface RealtimeDataProvider {
    public void init(Schema schema, String lastVersion);
    public void startProvider();
    public void stopProvider();
    public DataWithVersion next();
    public void commit(String version);
}
