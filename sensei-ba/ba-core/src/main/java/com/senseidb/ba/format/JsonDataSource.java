package com.senseidb.ba.format;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import com.alibaba.fastjson.JSONObject;
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
    public static class JsonIterator extends AbstractFileIterator {
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
        private Object transform(com.alibaba.fastjson.JSONArray value) throws JSONException {
            Object[] ret = new Object[value.size()];
            for(int i =0; i < value.size(); i++) {
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
                        if (value instanceof com.alibaba.fastjson.JSONArray) {
                            map.put(key, transform((com.alibaba.fastjson.JSONArray) value)); 
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
