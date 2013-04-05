package com.senseidb.ba.plugins;

import java.util.Arrays;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.senseidb.search.req.ErrorType;
import com.senseidb.search.req.RequestPostProcessor;
import com.senseidb.search.req.SenseiError;

/**
 * Removes group by and queries that are not supported in SenseiBA
 *
 */
public class BARequestPostProcessor implements RequestPostProcessor {
  @Override
  public List<SenseiError> process(JSONObject senseiRequest) {
    senseiRequest.remove("groupBy");
    Object query = senseiRequest.remove("query");
    if (query != null) {
      return Arrays.asList(new SenseiError("Queries are not supported for SenseiBA", ErrorType.JsonParsingError));
    }
    if (senseiRequest.optInt("size", 10) > 20000) {
      try {
        
        senseiRequest.put("size", 20000);
        return Arrays.asList(new SenseiError("Size shouldn't be bigger than 20000", ErrorType.BoboExecutionError));
      } catch (JSONException e) {
       throw new RuntimeException(e);
      }
    }
    return null;
  }

}
