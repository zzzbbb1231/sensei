package com.senseidb.ba.realtime.indexing;


public interface ShardingStrategy {   
    public int calculateShard(DataWithVersion dataWithVersion);
    public static class AcceptAllShardingStrategy implements ShardingStrategy {
      
      public int calculateShard(DataWithVersion dataWithVersion) {
        return 0;
      }
    }
}
