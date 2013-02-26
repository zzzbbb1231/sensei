package com.senseidb.ba.realtime.indexing;

import org.springframework.util.Assert;

import com.senseidb.ba.realtime.Schema;

public class FieldShardingStrategy implements ShardingStrategy {
  private int maxPartitionId;
  private int fieldIndex = -1; 
  public void init(Schema schema, int maxPartitionId, String fieldName) {
    this.maxPartitionId = maxPartitionId;
    
    for (int i = 0; i < schema.getColumnNames().length; i++) {
      if (schema.getColumnNames()[i].equalsIgnoreCase(fieldName)) {
        fieldIndex = i;
        break;
      }
    }
    Assert.state(fieldIndex != -1, "Could not find " + fieldName + " in the schema");
  }
  @Override
  public int calculateShard(DataWithVersion dataWithVersion) {
    Object obj = dataWithVersion.getValues()[fieldIndex];
    if (obj == null) {
      return 0;
    }
    return Math.abs(((Number) obj).intValue()) % (maxPartitionId + 1);
  }

}
