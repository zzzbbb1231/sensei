package com.senseidb.ba.realtime.indexing;

import java.util.Map;

import org.springframework.util.Assert;

import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.Schema;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.plugin.SenseiPluginRegistry;

public class IndexConfig {
    private int capacity;
    private long refreshTime;
    private int bufferSize = 0;
    private Schema schema;
    private ReusableIndexObjectsPool indexObjectsPool;
    private String indexDir;
    private String clusterName = "";;
    private int numServingPartitions;
    private String[] sortedColumns;
    private ReadMode readMode;
    public IndexConfig(int capacity, long refreshTime, int bufferSize, Schema schema, String indexDir, String clusterName,
        int numServingPartitions, String[] sortedColumns) {
      super();
      this.capacity = capacity;
      this.refreshTime = refreshTime;
      this.bufferSize = bufferSize;
      this.schema = schema;
      this.indexDir = indexDir;
      this.clusterName = clusterName;
      this.numServingPartitions = numServingPartitions;
      this.sortedColumns = sortedColumns;
    }
    public IndexConfig() {
      super();      
   
    }
    public void init(Schema schema, int capacity) {
      indexObjectsPool = new ReusableIndexObjectsPool();
      indexObjectsPool.init(schema, capacity);
    }
    public int getCapacity() {
      return capacity;
    }
    public long getRefreshTime() {
      return refreshTime;
    }
    public int getBufferSize() {
      return bufferSize;
    }
    public Schema getSchema() {
      return schema;
    }
    public ReusableIndexObjectsPool getIndexObjectsPool() {
      return indexObjectsPool;
    }
    public void setCapacity(int capacity) {
      this.capacity = capacity;
    }
    public void setRefreshTime(long refreshTime) {
      this.refreshTime = refreshTime;
    }
    public void setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
    }
    public void setSchema(Schema schema) {
      this.schema = schema;
    }
    public void setIndexObjectsPool(ReusableIndexObjectsPool indexObjectsPool) {
      this.indexObjectsPool = indexObjectsPool;
    }
    public String getIndexDir() {
      return indexDir;
    }
    public String getClusterName() {
      return clusterName;
    }
    public int getNumServingPartitions() {
      return numServingPartitions;
    }
    public void setNumServingPartitions(int numServingPartitions) {
      this.numServingPartitions = numServingPartitions;
    }
    
    
    public String[] getSortedColumns() {
      return sortedColumns;
    }
    public ReadMode getReadMode() {
      return readMode;
    }
    public void setIndexDir(String indexDir) {
      this.indexDir = indexDir;
    }
    public void setClusterName(String clusterName) {
      this.clusterName = clusterName;
    }
    public void setSortedColumns(String[] sortedColumns) {
      this.sortedColumns = sortedColumns;
    }
    public void setReadMode(ReadMode readMode) {
      this.readMode = readMode;
    }
    public static IndexConfig valueOf(Map<String, String> config, SenseiPluginRegistry pluginRegistry, Schema schema) {
      IndexConfig ret = new IndexConfig();
      ret.capacity = getIntConfig(config, "capacity", true);
      ret.refreshTime = getLongConfig(config, "refreshTime", true);
      ret.bufferSize =  getIntConfig(config, "bufferSize", true);
     
     
      ret.indexDir  = getStringConfig(config, "indexDir", false);
      if (ret.indexDir == null) {
        ret.indexDir =  pluginRegistry.getConfiguration().getString("sensei.index.directory");
      }
      Assert.notNull(ret.indexDir);
      ret.clusterName = getStringConfig(config, "clusterName", false);
      if (ret.clusterName == null) {
        ret.clusterName =  pluginRegistry.getConfiguration().getString(SenseiConfParams.SENSEI_CLUSTER_NAME);
      }
      Assert.notNull(ret.clusterName);
      ret.numServingPartitions = getIntConfig(config, "numServingPartitions", true);;
      String sortColumns = getStringConfig(config, "sortedColumns", false);
      if (sortColumns == null) {
        ret.sortedColumns = new String[0];
      } else {
        ret.sortedColumns = sortColumns.split(",");
      }  
      String readModeStr = getStringConfig(config, "readMode", false);
      if (readModeStr != null) {
        ret.readMode = ReadMode.valueOf(readModeStr);
      } else {
        ret.readMode = ReadMode.Heap;
      }
      ret.setSchema(schema);
      ret.init(schema,  ret.capacity);
      return ret;
      
    }
    private static String getStringConfig(Map<String, String> config, String key) {
      return getStringConfig(config, key, true);
    }
    private static String getStringConfig(Map<String, String> config, String key, boolean notNull) {
      String ret = config.get(key);
      if (notNull) {
        Assert.notNull(ret);
      }
      return ret;
    }
    private static Long getLongConfig(Map<String, String> config, String key, boolean notNull) {
      String ret = config.get(key);
      if (notNull) {
        Assert.notNull(ret);
        return Long.parseLong(ret);
      } 
      if (ret == null) return null;
      return Long.parseLong(ret);
    }
    private static Integer getIntConfig(Map<String, String> config, String key, boolean notNull) {
      String ret = config.get(key);
      if (notNull) {
        Assert.notNull(ret);
        return Integer.parseInt(ret);
      } 
      if (ret == null) return null;
      return Integer.parseInt(ret);
    }
}
