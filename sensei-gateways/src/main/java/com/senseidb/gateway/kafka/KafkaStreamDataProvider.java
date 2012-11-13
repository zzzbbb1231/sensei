package com.senseidb.gateway.kafka;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaMessageStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.indexing.DataSourceFilter;

public class KafkaStreamDataProvider extends StreamDataProvider<JSONObject>{
  private static Logger logger = Logger.getLogger(KafkaStreamDataProvider.class);

  private  String _topic;
  private  String _consumerGroupId;
  private Properties _kafkaConfig;
  protected ConsumerConnector _consumerConnector;
  private ConsumerIterator<Message> _consumerIterator;
  private ThreadLocal<DecimalFormat> formatter = new ThreadLocal<DecimalFormat>() {
    protected DecimalFormat initialValue() {
      return new DecimalFormat("00000000000000000000");
    }
  };
  
    private  String _zookeeperUrl;
    private  int _kafkaSoTimeout;
    private volatile boolean _started = false;
    private  DataSourceFilter<DataPacket> _dataConverter;
  
  public KafkaStreamDataProvider(Comparator<String> versionComparator,String zookeeperUrl,int soTimeout,int batchSize,
                                 String consumerGroupId,String topic,long startingOffset,DataSourceFilter<DataPacket> dataConverter){
    this(versionComparator, zookeeperUrl, soTimeout, batchSize, consumerGroupId, topic, startingOffset, dataConverter, new Properties());

  }
  public KafkaStreamDataProvider() {
    super(ZoieConfig.DEFAULT_VERSION_COMPARATOR);

  }
  public KafkaStreamDataProvider(Comparator<String> versionComparator,String zookeeperUrl,int soTimeout,int batchSize,
                                 String consumerGroupId,String topic,long startingOffset,DataSourceFilter<DataPacket> dataConverter,Properties kafkaConfig){
    super(versionComparator);
    _consumerGroupId = consumerGroupId;
    _topic = topic;
    super.setBatchSize(batchSize);
    _zookeeperUrl = zookeeperUrl;
    _kafkaSoTimeout = soTimeout;
    _consumerConnector = null;
    _consumerIterator = null;

    _kafkaConfig = kafkaConfig;
    if (kafkaConfig == null) {
      kafkaConfig = new Properties();
    }

    _dataConverter = dataConverter;
    if (_dataConverter == null){
      throw new IllegalArgumentException("kafka data converter is null");
    }
  }
  public void commit() {
    _consumerConnector.commitOffsets();
  }
  @Override
  public void setStartingOffset(String version){
  }
  
  @Override
  public DataEvent<JSONObject> next() {
    if (!_started) return null;

    try
    {
      if (!_consumerIterator.hasNext())
        return null;
    }
    catch (Exception e)
    {
      // Most likely timeout exception - ok to ignore
      return null;
    }

    Message msg = _consumerIterator.next();
    if (logger.isDebugEnabled()){
      logger.debug("got new message: "+msg);
    }
    long version = getNextVersion();
    
    JSONObject data;
    try {
      int size = msg.payloadSize();
      ByteBuffer byteBuffer = msg.payload();
      byte[] bytes = new byte[size];
      byteBuffer.get(bytes,0,size);
      data = _dataConverter.filter(new DataPacket(bytes,0,size));
      
      if (logger.isDebugEnabled()){
        logger.debug("message converted: "+data);
      }
      return new DataEvent<JSONObject>(data, getStringVersionRepresentation(version));
    } catch (Exception e) {
      logger.error(e.getMessage(),e);
      return null;
    }
  }
  public long getNextVersion() {
    return System.currentTimeMillis();
  }
  public String getStringVersionRepresentation(long version) {
    return formatter.get().format(version);
  }
  @Override
  public void reset() {
  }

  @Override
  public void start() {
    Properties props = new Properties();
    props.put("zk.connect", _zookeeperUrl);
    //props.put("consumer.timeout.ms", _kafkaSoTimeout);
    props.put("groupid", _consumerGroupId);

    for (String key : _kafkaConfig.stringPropertyNames()) {
      props.put(key, _kafkaConfig.getProperty(key));
    }

    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    _consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(_topic, 1);
    Map<String, List<KafkaMessageStream<Message>>> topicMessageStreams =
        _consumerConnector.createMessageStreams(topicCountMap);
    List<KafkaMessageStream<Message>> streams = topicMessageStreams.get(_topic);
    KafkaMessageStream<Message> kafkaMessageStream = streams.iterator().next();
    _consumerIterator = kafkaMessageStream.iterator();
    super.start();
    _started = true;
  }

  @Override
  public void stop() {
    _started = false;

    try
    {
      if (_consumerConnector!=null){
        _consumerConnector.shutdown();
      }
    }
    finally
    {
      super.stop();
    }
  }  
}
