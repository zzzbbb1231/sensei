package com.senseidb.ba.realtime.indexing.providers;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.ba.realtime.Schema;
import com.senseidb.ba.realtime.indexing.DataWithVersion;
import com.senseidb.ba.realtime.indexing.RealtimeDataProvider;

public class SenseiProviderAdapter implements RealtimeDataProvider {
  private final StreamDataProvider<JSONObject> provider;
  private Schema schema;

  public SenseiProviderAdapter(StreamDataProvider<JSONObject> provider ) {
    this.provider = provider;
    if (provider != null) {
     
       
    }
  }
  @Override
  public void init(Schema schema, String lastVersion) {
    this.schema = schema;
    
  }

  @Override
  public void start() {
    try {
      Field field = StreamDataProvider.class.getDeclaredField("_thread");
      field.setAccessible(true);
      Class<?> cls = Class.forName("proj.zoie.impl.indexing.StreamDataProvider$DataThread");
      Constructor<?> constructor = cls.getDeclaredConstructors()[0];
      constructor.setAccessible(true);
      Object obj = constructor.newInstance(new StreamDataProvider(ZoieConfig.DEFAULT_VERSION_COMPARATOR) {

        @Override
        public DataEvent next() {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public void setStartingOffset(String version) {
          // TODO Auto-generated method stub
          
        }

        @Override
        public void reset() {
          // TODO Auto-generated method stub
          
        }
        
      });
      field.set(provider, obj);
      Method method = cls.getMethod("start");
      method.setAccessible(true);
      method.invoke(obj);
      provider.start();
      method = cls.getDeclaredMethod("terminate");
      method.setAccessible(true);
      method.invoke(obj);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
   
    
  }

  @Override
  public void stop() {
    //provider.stop();
    
  }
  int count;
  @Override
  public DataWithVersion next() {
    final DataEvent<JSONObject> next = provider.next();
   
    if (next == null) {
      return null;
    }
    
    return new DataWithVersion() {
      @Override
      public String getVersion() {
        return next.getVersion();
      }
      
      @Override
      public Object[] getValues() {
        return schema.fromJson(next.getData());
      }
    };
  }

  @Override
  public void commit(String version) {
    if (provider != null) {
      try {
        Method method = provider.getClass().getMethod("commit");
       if (method != null) {
        method.setAccessible(true);
        method.invoke(provider);
       }
       
      
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

}
