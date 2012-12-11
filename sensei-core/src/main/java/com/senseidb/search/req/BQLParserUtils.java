package com.senseidb.search.req;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import antlr.collections.List;

import com.senseidb.util.JSONUtil;
import com.senseidb.util.Pair;

public class BQLParserUtils {
  public static void decorateWithMapReduce(JSONObject jsonObj, java.util.List<Pair<String, String>> aggreagationFunctions, JSONObject groupBy,
      String functionName, JSONObject parameters) {
    try {
      if (aggreagationFunctions == null) {
        aggreagationFunctions = new ArrayList<Pair<String,String>>();
      }
     
      JSONArray array = new JSONUtil.FastJSONArray();
      if (groupBy == null) {
        for (Pair<String, String> pair: aggreagationFunctions) {
          JSONObject props = new JSONUtil.FastJSONObject();

          props.put("column", pair.getSecond());
          props.put("mapReduce", pair.getFirst());
          array.put(props);
        }
      } else {
        for (Pair<String, String> pair: aggreagationFunctions) {
          JSONObject props = new JSONUtil.FastJSONObject();
          props.put("function", pair.getFirst());
          props.put("metric", pair.getSecond());
          props.put("columns", groupBy.optJSONArray("columns"));
          props.put("mapReduce", "sensei.groupBy");
          array.put(props);
        }
      }
      if (functionName != null) {
        if (parameters == null) {
          parameters = new JSONUtil.FastJSONObject();
        }
        
        parameters.put("mapReduce", functionName);
        array.put(parameters);
      }
      JSONObject mapReduce = new JSONUtil.FastJSONObject();
      if (array.length() == 0) {
        return;
      }
      if (array.length() == 1) {
        JSONObject props = array.getJSONObject(0);
        mapReduce.put("function", props.get("mapReduce"));
        mapReduce.put("parameters", props);        
      } else {
        mapReduce.put("function", "sensei.composite");
        JSONObject props = new JSONUtil.FastJSONObject();
        props.put("array", array);
        mapReduce.put("parameters", props);
      }
      jsonObj.put("mapReduce", mapReduce);
      // we need to remove group by since it's in Map reduce
      //jsonObj.remove("groupBy");
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }
}
