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
    private Set<JsonIterator> iterators = new HashSet<JsonIterator>();
    public JsonDataSource(File jsonFile) {
        this.jsonFile = jsonFile;
        
    }
    @Override
    public Iterator<Map<String, Object>> newIterator() {
        JsonIterator ret = new JsonIterator(jsonFile);
        iterators.add(ret);
        return ret;
    }

    @Override
    public void closeCurrentIterators() {
        for (JsonIterator iterator : iterators) {
            iterator.close();
        }
        
    }
    public static class JsonIterator extends AbstractIterator {
        public JsonIterator(File jsonFile)  {
            super(jsonFile);
            Field mapField;
            try {
                mapField = JSONObject.class.getDeclaredField("map");
                mapField.setAccessible(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            
        }
        private Object transform(JSONArray value) throws JSONException {
            Object[] ret = new Object[value.length()];
            for(int i =0; i < value.length(); i++) {
                ret[i] = value.get(i);
            }
            return ret;
        }
        protected Map<String, Object> processLine(String line) {
            if (line != null && line.contains("{")) {
                try {
                    FastJSONObject fastJSONObject = new JSONUtil.FastJSONObject(line);
                    JSONObject innerJSONObject = fastJSONObject.getInnerJSONObject();
                    Field mapField = JSONObject.class.getDeclaredField("map");
                    mapField.setAccessible(true);
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
            return null;
        }
    }
}
