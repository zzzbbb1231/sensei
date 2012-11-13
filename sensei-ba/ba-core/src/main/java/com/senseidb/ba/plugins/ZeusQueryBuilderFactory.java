package com.senseidb.ba.plugins;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.json.JSONObject;

import com.senseidb.search.node.SenseiQueryBuilder;
import com.senseidb.search.node.SenseiQueryBuilderFactory;
import com.senseidb.search.node.impl.AbstractJsonQueryBuilderFactory;
import com.senseidb.search.query.filters.FilterConstructor;
import com.senseidb.search.req.SenseiQuery;

public class ZeusQueryBuilderFactory extends AbstractJsonQueryBuilderFactory {
 
 
  @Override
  public SenseiQueryBuilder buildQueryBuilder(JSONObject jsonQuery) {
    final  JSONObject filter =  jsonQuery != null ? jsonQuery.optJSONObject("filter") : null;    
    return new SenseiQueryBuilder() {
      
      @Override
      public Query buildQuery() throws ParseException {
      
         return new MatchAllDocsStaticQuery();
       
      }
      
      @Override
      public Filter buildFilter() throws ParseException {
        if (filter != null) {
          try {
            return FilterConstructor.constructFilter(filter, null);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
        return null;
      }
    };
  }

}
