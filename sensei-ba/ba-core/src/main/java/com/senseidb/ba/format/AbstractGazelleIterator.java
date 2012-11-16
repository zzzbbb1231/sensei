package com.senseidb.ba.format;

import java.util.Iterator;
import java.util.Map;

public abstract class AbstractGazelleIterator implements   Iterator<Map<String, Object>>{

  public AbstractGazelleIterator() {
    super();
  }

  @Override
  public void remove() {
      // TODO Auto-generated method stub
      
  }

  public abstract void close();

}