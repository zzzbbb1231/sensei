package com.senseidb.ba.realtime.indexing.providers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import org.json.JSONException;
import org.springframework.util.Assert;

import com.senseidb.ba.realtime.Schema;
import com.senseidb.ba.realtime.indexing.DataWithVersion;
import com.senseidb.ba.realtime.indexing.RealtimeDataProvider;
import com.senseidb.plugin.SenseiPlugin;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.senseidb.util.JSONUtil;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class KafkaRealtimeDataProvider implements RealtimeDataProvider, SenseiPlugin {
  private Properties props;
  private List<String> topics;
  private ConsumerConnector consumerConnector;
  private Iterator<Message> messageIterator;
  private Schema schema;
  private RealtimeDataTransformer dataTransformer;
  private AtomicLong versionCounter = new AtomicLong();
  @Override
  public void init(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
     props = new Properties();
     for(String key : config.keySet()) {
       if (key.startsWith("kafka.") && !"kafka.topics".equalsIgnoreCase(key)) {
         props.put(key.substring("kafka.".length()), config.get(key));
       }
     }
     String topicsStr = config.get("kafka.topics");
     Assert.notNull(topicsStr, "kafka.topics parameter shouldn't be null");
     topics = new ArrayList<String>();
     for (String str : topicsStr.split(",")) {
       if (str == null) {
         continue;
       }
       str = str.trim();
       if (str.length() > 0) {
         topics.add(str);
       }
     }
     if (config.get("dataTransformer") != null) {
       try {
        dataTransformer = (RealtimeDataTransformer) Class.forName(config.get("dataTransformer")).newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } 
     }     
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void init(Schema schema, String lastVersion) {
    this.schema = schema;
    
  }

  @Override
  public void startProvider() {
    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    messageIterator = KafkaUtils.createIterator(consumerConnector, topics);
    
  }

  @Override
  public void stopProvider() {
    try
    {
      if (KafkaUtils.getExecutorService() != null)
      {
        KafkaUtils.getExecutorService().shutdownNow();
      }
    }
    finally
    {
        consumerConnector.shutdown();
    }
    
  }

  @Override
  public DataWithVersion next() {
    if (messageIterator == null || !messageIterator.hasNext()) {
      return null;
    }
    Message message = messageIterator.next();
    if (message == null) {
      return null;
    }
    byte[] bytes = new byte[message.payloadSize()];
    message.payload().get(bytes,0,bytes.length);
    if (dataTransformer != null) {      
      return new DataWithVersion.DataWithVersionImpl(dataTransformer.transform(bytes, schema), versionCounter.incrementAndGet()) ;
    } else {
      try {
        FastJSONObject json = new JSONUtil.FastJSONObject(new String(bytes));
        return new DataWithVersion.DataWithVersionImpl(schema.fromJson(json), versionCounter.incrementAndGet());
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void commit(String version) {
    consumerConnector.commitOffsets();
    
  }

}
