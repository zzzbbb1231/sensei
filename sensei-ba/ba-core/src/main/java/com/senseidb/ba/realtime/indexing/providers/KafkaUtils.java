package com.senseidb.ba.realtime.indexing.providers;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.KafkaMessageStream;
import kafka.message.Message;

import org.apache.log4j.Logger;

public class KafkaUtils {
  private static Logger logger = Logger.getLogger(KafkaUtils.class);  
  
  private static ExecutorService _executorService;

  public static Iterator<Message> createIterator(kafka.javaapi.consumer.ConsumerConnector consumerIterator,  List<String> topics) {
     Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
     for (String topic : topics)
     {
       topicCountMap.put(topic, 1);
     }
     Map<String, List<KafkaMessageStream<Message>>> topicMessageStreams =
         consumerIterator.createMessageStreams(topicCountMap);


     final ArrayBlockingQueue<Message> queue = new ArrayBlockingQueue<Message>(120, false);

     int streamCount = 0;
     for (List<KafkaMessageStream<Message>> streams : topicMessageStreams.values())
     {
       for (KafkaMessageStream<Message> stream : streams)
       {
         ++streamCount;
       }
     }
     _executorService = Executors.newFixedThreadPool(streamCount);

     for (List<KafkaMessageStream<Message>> streams : topicMessageStreams.values())
     {
       for (KafkaMessageStream<Message> stream : streams)
       {
         final KafkaMessageStream<Message> messageStream = stream;
         _executorService.execute(new Runnable()
           {
             @Override
             public void run()
             {
               logger.info("Kafka consumer thread started: " + Thread.currentThread().getId());
               try
               {
                 for (Message message : messageStream)
                 {
                   queue.put(message);
                 }
               } catch (InterruptedException ie) {
                 Thread.currentThread().interrupt();
                 return;
               }
               catch(Exception e)
               {
                 // normally it should the stop interupt exception.
                 logger.error(e.getMessage(), e);
               }
               logger.info("Kafka consumer thread ended: " + Thread.currentThread().getId());
             }
           }
         );
       }
     }

     Iterator<Message> _consumerIterator = new Iterator<Message>()
     {
       private Message message = null;

       @Override
       public boolean hasNext()
       {
         if (message != null)  return true;

         try
         {
           message = queue.poll(1, TimeUnit.SECONDS);
         }
         catch(InterruptedException ie)
         {
           return false;
         }

         if (message != null)
         {
           return true;
         }
         else
         {
           return false;
         }
       }

       @Override
       public Message next()
       {
         if (hasNext())
         {
           Message res = message;
           message = null;
           return res;
         }
         else
         {
           throw new NoSuchElementException();
         }
       }

       @Override
       public void remove()
       {
         throw new UnsupportedOperationException("not supported");
       }
     };
     return _consumerIterator;
   }

  public static ExecutorService getExecutorService() {
    return _executorService;
  }
  
}
