package com.senseidb.ba.realtime.indexing;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.springframework.util.Assert;
import org.w3c.dom.Document;

import com.senseidb.ba.gazelle.ColumnType;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.ba.realtime.ReusableIndexObjectsPool;
import com.senseidb.ba.realtime.Schema;
import com.senseidb.conf.SchemaConverter;
import com.senseidb.conf.SenseiConfParams;
import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiSchema.FieldDefinition;
import com.senseidb.plugin.SenseiPluginRegistry;

public class IndexConfig {
  private int capacity;
  private long refreshTime;
  private int bufferSize = 0;
  private Schema schema;
  private ReusableIndexObjectsPool indexObjectsPool;
  private String indexDir;
  private String clusterName = "";
  private int numServingPartitions;
  private String[] sortedColumns;
  private ReadMode readMode;
  private SenseiSchema senseiSchema;
  private String shardedColumn;
  private int maxPartitionId;
  private int partition;
  private TimeUnit retentionTimeUnit;
  private long retentionDuration = -1; 
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

  public SenseiSchema getSenseiSchema() {
    return senseiSchema;
  }

  public void setSenseiSchema(SenseiSchema senseiSchema) {
    this.senseiSchema = senseiSchema;
  }

  public static IndexConfig valueOf(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
    IndexConfig ret = new IndexConfig();
    ret.capacity = getIntConfig(config, "capacity", true);
    ret.refreshTime = getLongConfig(config, "refreshTime", true);
    ret.bufferSize = getIntConfig(config, "bufferSize", true);

    ret.indexDir = getStringConfig(config, "indexDir", false);
    if (ret.indexDir == null) {
      ret.indexDir = pluginRegistry.getConfiguration().getString("sensei.index.directory");
    }
    Assert.notNull(ret.indexDir);
    ret.clusterName = getStringConfig(config, "clusterName", false);
    if (ret.clusterName == null) {
      ret.clusterName = pluginRegistry.getConfiguration().getString(SenseiConfParams.SENSEI_CLUSTER_NAME);
    }
    String retentionStr= getStringConfig(config, "retention.timeUnit", false);
    if (retentionStr != null && retentionStr.length() > 0) {
      ret.retentionTimeUnit = TimeUnit.valueOf(retentionStr.toUpperCase().trim());
      ret.retentionDuration = getLongConfig(config, "retention.duration", true);
    }
    Assert.notNull(ret.clusterName);
    ret.numServingPartitions = getIntConfig(config, "numServingPartitions", true);
    ;
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
    ret.shardedColumn = getStringConfig(config, "shardedColumn", false);
    ret.maxPartitionId = pluginRegistry.getConfiguration().getInt("sensei.index.manager.default.maxpartition.id", 0);
    ret.partition = pluginRegistry.getConfiguration().getInt("sensei.node.partitions", 0);
    String schemaPath = getStringConfig(config, "schemaPath", false);
    String schema = getStringConfig(config, "schema", false);
    if (schemaPath != null || schema == null) {
      File schemaFile = null;
      if (schemaPath != null) {
        schemaFile = new File(schemaPath);
        if (!schemaFile.exists()) {
          try {
            schemaFile = new File(IndexConfig.class.getClassLoader().getResource(schemaPath).toURI());
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }
      } else {
        File baseDir = ((PropertiesConfiguration) pluginRegistry.getConfiguration()).getFile().getParentFile();
        schemaFile = new File(baseDir, "schema.xml");
      }

      if (schemaFile != null) {

        FileInputStream fileInputStream = null;
        try {
          fileInputStream = new FileInputStream(schemaFile);
          DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
          dbf.setIgnoringComments(true);
          DocumentBuilder db = dbf.newDocumentBuilder();
          Document schemaXml = db.parse(fileInputStream);
          schemaXml.getDocumentElement().normalize();
          JSONObject jsonObject = SchemaConverter.convert(schemaXml);
          ret.senseiSchema = SenseiSchema.build(jsonObject);
          ret.setSchema(convert(ret.senseiSchema));
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        } finally {
          IOUtils.closeQuietly(fileInputStream);
        }
      }
    } else {
      
      try {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setIgnoringComments(true);
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document schemaXml = db.parse(new ByteArrayInputStream(schema.getBytes()));
        schemaXml.getDocumentElement().normalize();
        JSONObject jsonObject = SchemaConverter.convert(schemaXml);
        ret.senseiSchema = SenseiSchema.build(jsonObject);
        ret.setSchema(convert(ret.senseiSchema));
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    ret.init(ret.schema, ret.capacity);
    Set<String> columnNames = new HashSet<String>();
    for (String column : ret.schema.getColumnNames()) {
      columnNames.add(column.trim());
    }
    for (String sortedColumn : ret.sortedColumns) {
     Assert.state(columnNames.contains(sortedColumn.trim()), "The sortedColumn " + sortedColumn + " doesn't match columns from the schema - " + columnNames);
    }
    return ret;

  }

  public static Schema convert(SenseiSchema senseiSchema) {
    List<String> columns = new ArrayList<String>();
    List<ColumnType> types = new ArrayList<ColumnType>();
    for (String column : senseiSchema.getFieldDefMap().keySet()) {
      FieldDefinition fieldDefinition = senseiSchema.getFieldDefMap().get(column);
      ColumnType type = ColumnType.STRING;
      if (fieldDefinition.type != null) {
        type = ColumnType.valueOf(fieldDefinition.type);
      }
      if (fieldDefinition.isMulti) {
        type = ColumnType.valueOfArrayType(type);
      }
      types.add(type);
      columns.add(column);
    }

    Schema schema = new Schema(columns.toArray(new String[columns.size()]), types.toArray(new ColumnType[types.size()]));
    return schema;
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
    if (ret == null)
      return null;
    return Long.parseLong(ret);
  }

  private static Integer getIntConfig(Map<String, String> config, String key, boolean notNull) {
    String ret = config.get(key);
    if (notNull) {
      Assert.notNull(ret);
      return Integer.parseInt(ret);
    }
    if (ret == null)
      return null;
    return Integer.parseInt(ret);
  }

  public String getShardedColumn() {
    return shardedColumn;
  }

  public int getMaxPartitionId() {
    return maxPartitionId;
  }

  public int getPartition() {
    return partition;
  }

  public void setShardedColumn(String shardedColumn) {
    this.shardedColumn = shardedColumn;
  }

  public void setMaxPartitionId(int maxPartitionId) {
    this.maxPartitionId = maxPartitionId;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  public TimeUnit getRetentionTimeUnit() {
    return retentionTimeUnit;
  }

  public long getRetentionDuration() {
    return retentionDuration;
  }

  public void setRetentionTimeUnit(TimeUnit retentionTimeUnit) {
    this.retentionTimeUnit = retentionTimeUnit;
  }

  public void setRetentionDuration(long duration) {
    this.retentionDuration = duration;
  }

}
