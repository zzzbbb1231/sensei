package com.senseidb.ba.realtime;

import java.text.DecimalFormat;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.creators.DictionaryCreator;
import com.senseidb.ba.realtime.indexing.DataWithVersion;
import com.senseidb.ba.realtime.indexing.RealtimeDataProvider;

public class TestDataProvider implements RealtimeDataProvider{
  DecimalFormat decimalFormat = new DecimalFormat(DictionaryCreator.DEFAULT_FORMAT_STRING_MAP.get(ColumnType.LONG));
  int position  = -1;;

  

  

  @Override
  public DataWithVersion next() {
    position++;
    if (position > 199998) {
      return null;
    }
   
    return new DataWithVersion() {
      
      @Override
      public String getVersion() {
        return "" + position;
      }
      
      @Override
      public Object[] getValues() {
        int id = 1000000 - position;
        return new Object[] {id,  String.valueOf(id/10) };
      }
    };
  }

  @Override
  public void commit(String version) {
    
  }



  @Override
  public void init(Schema schema, String lastVersion) {    
    
  }

  @Override
  public void start() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void stop() {
    // TODO Auto-generated method stub
    
  }
}
