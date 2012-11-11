package com.senseidb.ba;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.json.JSONArray;
import org.json.JSONException;

import com.alibaba.fastjson.JSONObject;
import com.senseidb.ba.gazelle.creators.GazelleDataSource;
import com.senseidb.util.JSONUtil;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class JsonDataSource implements GazelleDataSource {
    private final File jsonFile;
    private Set<JsonFileIterator> iterators = new HashSet<JsonDataSource.JsonFileIterator>();
    public JsonDataSource(File jsonFile) {
        this.jsonFile = jsonFile;
        
    }
    @Override
    public Iterator<Map<String, Object>> newIterator() {
        JsonFileIterator ret = new JsonFileIterator(jsonFile);
        iterators.add(ret);
        return ret;
    }

    @Override
    public void closeCurrentIterators() {
        for (JsonFileIterator iterator : iterators) {
            iterator.close();
        }
        
    }
    public static class JsonFileIterator implements  Iterator<Map<String, Object>> {
        private LineIterator lineIterator;
        private Field mapField;
        Map<String, Object> _next;
        public JsonFileIterator(File jsonFile)  {
            try { 
            lineIterator = FileUtils.lineIterator(jsonFile);
                  mapField = JSONObject.class.getDeclaredField("map");
                 mapField.setAccessible(true);
             } catch (Exception e) {
                 throw new RuntimeException(e);
             }
        }
        
        public Map<String, Object> _next() {
            while(lineIterator.hasNext()) {
                String line = lineIterator.next();
                if (line != null && line.contains("{")) {
                    try {
                        FastJSONObject fastJSONObject = new JSONUtil.FastJSONObject(line);
                        JSONObject innerJSONObject = fastJSONObject.getInnerJSONObject();
                        Map map = (Map) mapField.get(innerJSONObject);
                        for (Object key : map.keySet()) {
                            Object value = map.get(key);
                            if (value instanceof JSONArray) {
                                map.put(key, transform((JSONArray) value)); 
                            }
                        }
                        return map;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return null;
        }
        private Object transform(JSONArray value) throws JSONException {
            Object[] ret = new Object[value.length()];
            for(int i =0; i < value.length(); i++) {
                ret[i] = value.get(i);
            }
            return ret;
        }

        @Override
        public boolean hasNext() {
            if (_next == null) {
                _next = _next();
            }
            return _next != null;
        }

        @Override
        public Map<String, Object> next() {
            Map<String, Object> ret = null;
            if (_next == null) {
                _next = _next();
            }
            ret = _next;
            _next = null;
            return ret;
        }

        @Override
        public void remove() {
            // TODO Auto-generated method stub
            
        }
        public void close() {
            lineIterator.close();
        }
    }
}
