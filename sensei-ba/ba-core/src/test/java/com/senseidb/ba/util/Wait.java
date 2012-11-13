package com.senseidb.ba.util;

public abstract class Wait {
  public Wait(long timeout) {
     long time = System.currentTimeMillis();
     while (!until()) {
       if (System.currentTimeMillis() - time > timeout) {
         throw new IllegalStateException("Timeout occured");
       }
       try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
     }
  }
  public Wait() {
    this(2000L);
 }
  public abstract boolean until();
  
  
  
}
