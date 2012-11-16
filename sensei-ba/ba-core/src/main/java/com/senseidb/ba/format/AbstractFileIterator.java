package com.senseidb.ba.format;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.json.JSONArray;
import org.json.JSONException;

import com.alibaba.fastjson.JSONObject;
import com.senseidb.util.JSONUtil;
import com.senseidb.util.JSONUtil.FastJSONObject;

public abstract class AbstractFileIterator extends AbstractGazelleIterator  {
    protected LineIterator lineIterator;
    
    Map<String, Object> _next;
    public AbstractFileIterator(File jsonFile)  {
        try { 
        lineIterator = FileUtils.lineIterator(jsonFile);
             
         } catch (Exception e) {
             throw new RuntimeException(e);
         }
    }
    
    public Map<String, Object> _next() {
        while(lineIterator.hasNext()) {
            String line = lineIterator.next();
            Map<String, Object> ret = processLine(line);
            if (ret != null) {
                return ret;
            }
        }
        return null;
    }

    protected abstract Map<String, Object> processLine(String line);
    @Override
    public void close() {
      lineIterator.close();
    
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
}


