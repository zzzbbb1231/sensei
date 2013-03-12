/*package com.senseidb.ba.realtime;

import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.senseidb.ba.util.TestUtil;
import com.senseidb.util.SingleNodeStarter;
*//**
 * You need to include jars from sensei-ba/config-example/src/main/resources/realtime-config-kafka/ext directory into the classpath
 *
 *//*
public class RealtimeIndexingFromKafkaTest  extends Assert {
  
  private static KafkaServer kafkaServer;
  private static Properties kafkaProps;
  private static File ConfDir1;
  @BeforeClass
  public static void setUp() throws Exception {
     ConfDir1 = new File("/home/vzhabiuk/work/sensei-ba/sensei/sensei-ba/config-example/src/main/resources/realtime-config-kafka");
    File kafkaServerFile = new File("/home/vzhabiuk/work/sensei-ba/sensei/sensei-ba/config-example/src/main/resources/realtime-config-kafka/ext/kafka-server.properties");
    
     kafkaProps = new Properties();
    kafkaProps.load(new FileReader(kafkaServerFile));
    
    File kafkaLogFile = new File(kafkaProps.getProperty("log.dir"));
    FileUtils.deleteDirectory(kafkaLogFile);
    FileUtils.deleteDirectory(new File("/tmp/realtimeKafkaIndex/test"));
    KafkaConfig kafkaConfig = new KafkaConfig(kafkaProps);
    kafkaServer = new KafkaServer(kafkaConfig);
    kafkaServer.startup();
    kafkaServer.shutdown();
    kafkaServer.awaitShutdown();
    Thread.sleep(500);
    kafkaServer = new KafkaServer(kafkaConfig);
    kafkaServer.startup();
    Properties props = new Properties();
    props.put("zk.connect", "localhost:2181");
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    final ProducerConfig producerConfig = new ProducerConfig(props);
    new Thread() {
      public void run() {
        try {
        Producer<String,Message> kafkaProducer = new Producer<String,Message>(producerConfig);
        File dataFiles = new File("/home/vzhabiuk/work/sensei-ba/sensei/sensei-ba/config-example/src/main/resources/realtime-config-http/data");
        List<ProducerData<String, Message>> msgList = new ArrayList<ProducerData<String, Message>>();
        for (File jsonFile : dataFiles.listFiles()) {
          JSONArray jsonArray = new JSONArray(FileUtils.readFileToString(jsonFile));
          for (int i = 0; i < jsonArray.length(); i++) {
            Message m = new Message(jsonArray.getJSONObject(i).toString().getBytes());
            ProducerData<String,Message> msg = new ProducerData<String,Message>("test1",m);
            for (int j = 0; j < 10000; j++) {
              msgList.add(msg);
            }
            kafkaProducer.send(msgList);
            msgList.clear();
          }
         
        }
        kafkaProducer.close();
        } catch (Exception ex) {
          ex.printStackTrace();
          throw new RuntimeException(ex);
        }
      };
    }.start();
    
    
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(Level.INFO);
    SingleNodeStarter.start(ConfDir1, 1000000000);
    
  }
  @Test
  public void test1() throws Exception {
    String req = "{\"bql\":\"select * \"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8080/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals(resp.getInt("numhits"), 10000);
  }
  @Test
  public void test2Restart() throws Exception {
    String req = "{\"bql\":\"select * \"}";
    JSONObject resp = TestUtil.search(new URL("http://localhost:8080/sensei"), new JSONObject(req).toString());
    System.out.println(resp.toString(1));
    assertEquals(resp.getInt("numhits"), 10000);
    Thread.sleep(500L);
    SingleNodeStarter.shutdown(); 
    FileUtils.deleteDirectory(new File("/tmp/realtimeKafkaIndex/test"));
    SingleNodeStarter.start(ConfDir1, 10000);
    assertEquals(resp.getInt("numhits"), 10000);
  }
  @AfterClass
  public static void tearDown() throws Exception {
    //Thread.sleep(5000);
    SingleNodeStarter.shutdown(); 
    kafkaServer.shutdown();
    kafkaServer.awaitShutdown();
    kafkaServer.CLEAN_SHUTDOWN_FILE();
    long time = System.currentTimeMillis();
    File kafkaLogFile = new File(kafkaProps.getProperty("log.dir"));
    FileUtils.deleteDirectory(kafkaLogFile);
    FileUtils.deleteDirectory(new File("/tmp/realtimeKafkaIndex/test"));
    //SingleNodeStarter.rmrf(new File("ba-index/ba-data"));
    //FileUtils.deleteDirectory(new File("ba-index/ba-data"));
  }
 
}
*/