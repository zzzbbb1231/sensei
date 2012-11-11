package com.senseidb.ba.gazelle.creators;

import java.util.Iterator;
import java.util.Map;

import com.senseidb.ba.gazelle.ColumnType;

public interface GazelleDataSource {
    public Iterator<Map<String, Object>> newIterator();
    public void closeCurrentIterators();
}
