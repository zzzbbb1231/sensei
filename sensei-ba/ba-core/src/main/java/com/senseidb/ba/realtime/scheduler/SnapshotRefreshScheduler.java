package com.senseidb.ba.realtime.scheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class SnapshotRefreshScheduler {
   private ScheduledExecutorService refreshService = Executors.newScheduledThreadPool(1);
   private long lastRefreshedTime;
   private volatile int lastRefreshedSize;
   private int batchSize;
   private int capacity;
   private long timeDelay;
   private final Object lock = new Object();
   long historicalRefreshTime = lastRefreshedTime;
   private volatile int currentSize = 0;
   private TimeRefreshJob timeRefreshJob = new TimeRefreshJob();
   private CountRefreshJob countRefreshJob = new CountRefreshJob();
   private boolean isCancelled = false;
   public void init(int batchSize, int capacity, long timeDelay) {
     
    this.batchSize = batchSize;
    this.capacity = capacity;
    this.timeDelay = timeDelay;
    lastRefreshedTime = System.currentTimeMillis();     
     historicalRefreshTime = lastRefreshedTime;
   }
   
   
   
   public void start() {
     lastRefreshedTime = System.currentTimeMillis();     
     historicalRefreshTime = lastRefreshedTime;
     if (timeDelay > 0) {
         refreshService.schedule(timeRefreshJob, timeDelay, TimeUnit.MILLISECONDS);
     }
   }
   public void stop() {
     synchronized(lock) {
       isCancelled = true;
     }
     refreshService.shutdownNow();
   }
   
   public abstract int refresh();
   
   
   public void sizeUpdated(int newSize) {
     currentSize = newSize;
     if (batchSize <= 0) {
         return;
     }
     if (currentSize -  lastRefreshedSize >= batchSize) {
       refreshService.submit(countRefreshJob);
     } 
   }
  
   
   public final class CountRefreshJob implements Callable<Void> {
     @Override
     public Void call() throws Exception {
       if (isCancelled) return null;
       synchronized(lock) {
         if (isCancelled) return null;
         if (currentSize -  lastRefreshedSize >= batchSize) {
           lastRefreshedSize = refresh();
           lastRefreshedTime = System.currentTimeMillis();
         }
       }
       return null;
     }
   }
   public final class TimeRefreshJob implements Callable<Void> {
     @Override
     public Void call() throws Exception {
       if (isCancelled) return null;
       synchronized(lock) {
         if (isCancelled) return null;
         if (historicalRefreshTime == lastRefreshedTime) {
           lastRefreshedSize = refresh();
           lastRefreshedTime = System.currentTimeMillis();
           historicalRefreshTime = lastRefreshedTime;
           refreshService.schedule(this, timeDelay, TimeUnit.MILLISECONDS);
         } else {
           long newDelay = timeDelay -(lastRefreshedTime - historicalRefreshTime);
           historicalRefreshTime = lastRefreshedTime;
           if (newDelay < 0) {
             newDelay = 0;
           }
           refreshService.schedule(this, newDelay, TimeUnit.MILLISECONDS);
         }
       }
       return null;
     }
   }
}
