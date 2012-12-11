package com.senseidb.ba.plugins;

import java.util.Arrays;
import java.util.List;

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
    return null;
  }

}
