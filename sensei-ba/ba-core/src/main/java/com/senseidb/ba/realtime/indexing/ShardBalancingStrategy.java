package com.senseidb.ba.realtime.indexing;

import com.senseidb.ba.gazelle.IndexSegment;

public class ShardBalancingStrategy {
  private int[] shardCount;
  public ShardBalancingStrategy(int numServingPartitions) {
    shardCount = new int[numServingPartitions];
  }  
  
  public int chooseShard(IndexSegment indexSegment) {
    int minPartition = -1;
    int minCount = Integer.MAX_VALUE;
    for (int i = 0; i < shardCount.length; i++) {
      if (shardCount[i] <  minCount) {
        minCount = shardCount[i];
        minPartition = i;
      }
    }
    shardCount[minPartition] += indexSegment.getLength();
    return minPartition;
  }
}
