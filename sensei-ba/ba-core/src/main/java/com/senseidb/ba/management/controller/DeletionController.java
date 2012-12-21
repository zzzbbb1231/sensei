package com.senseidb.ba.management.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.IZkChildListener;
import org.springframework.util.Assert;

import com.senseidb.ba.file.http.FileManagementServlet;
import com.senseidb.ba.gazelle.SegmentTimeType;
import com.senseidb.ba.management.SegmentInfo;
import com.senseidb.ba.management.SegmentTracker;
import com.senseidb.ba.management.SegmentUtils;
import com.senseidb.indexing.activity.time.Clock;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

public class DeletionController extends BaController {
  private volatile List<SegmentInfo> segments = null;
  private volatile long retention;
  private volatile TimeUnit timeUnit;
  private Integer maxPartition;
  private static final Counter deletedSegments = Metrics.newCounter(SegmentTracker.class, "deletedSegments");
  private static final Counter failedToDeleteSegments = Metrics.newCounter(SegmentTracker.class, "failedToDeleteSegments");
  private static final Counter executions = Metrics.newCounter(SegmentTracker.class, "executions");
  @Override
  public void execute() {
    executions.inc();
    logger.info("Started the job to delete segments");
    long time = System.currentTimeMillis();
    if (segments == null) {
      return;
    }
    for (SegmentInfo segmentInfo : segments) {
      if (segmentInfo == null) {
        return;
        
      }
      if (segmentInfo.getConfig() == null) {
        return;
        
      }
      if (segmentInfo.getConfig().get("segment.endTime") == null) {
        continue;
      }
      if (segmentInfo.getConfig().get("segment.time.Type") == null) {
        continue;
      }
      long endTime = Long.parseLong(segmentInfo.getConfig().get("segment.endTime"));
      SegmentTimeType segmentTimeType = SegmentTimeType.valueOf(segmentInfo.getConfig().get("segment.time.Type"));
      if (segmentTimeType.toMillis(endTime) + timeUnit.toMillis(retention) < Clock.getTime()) {
        deleteSegment(segmentInfo);
      }
    }
    logger.info("Finished the job to delete segments with total time " + (System.currentTimeMillis() - time) + " milliseconds");
  }

  private void deleteSegment(SegmentInfo segmentInfo) {
    try {
      logger.info("deleting segment - " + segmentInfo.getSegmentId() + " with props" + segmentInfo.toJson());
      int partition = FileManagementServlet.getPartition(segmentInfo.getSegmentId(), maxPartition);
      SegmentUtils.removeFromActiveSegments(zkClient, clusterName, partition, segmentInfo.getSegmentId());
      zkClient.deleteRecursive(SegmentUtils.getSegmentInfoPath(clusterName, segmentInfo.getSegmentId()));
      deletedSegments.inc();
    } catch (Exception ex) {
      logger.error("Couldn't delete the segment - " + segmentInfo.getSegmentId());
      failedToDeleteSegments.inc();
    }
    
  }

  private long getCurrentDaysSinceEpoch() {
    return System.currentTimeMillis() / (24 * 60 * 60 * 1000);
  }
@Override
public String getControllerName(SenseiPluginRegistry pluginRegistry, Map<String, String> config) {
  return "DeletionController";
}
  @Override
  protected void doBecomeMaster(Map<String, String> config, SenseiPluginRegistry pluginRegistry) {
     maxPartition = pluginRegistry.getConfiguration().getInt("sensei.index.manager.default.maxpartition.id", 0);
    Assert.notNull(maxPartition, "maxPartition parameter should be present");
    Assert.notNull(config.get("retention"));
    retention = Long.parseLong(config.get("retention"));
    if (config.get("timeUnit") != null) {
      timeUnit = TimeUnit.valueOf(config.get("timeUnit"));
    } else {
      timeUnit = TimeUnit.valueOf("DAYS");;
    }
    List<String> childs = zkClient.subscribeChildChanges(SegmentUtils.getSegmentInfoPath(clusterName), new IZkChildListener() {
    
    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
      List<SegmentInfo> newSegments = new ArrayList<SegmentInfo>();
      if (currentChilds == null) {
        return;
      }
      for (String segment : currentChilds) {
        newSegments.add(SegmentInfo.retrieveFromZookeeper(zkClient, clusterName, segment));
      }
      segments = newSegments;
    }
  });
    List<SegmentInfo> newSegments = new ArrayList<SegmentInfo>();
    if (childs == null) {
      return;
    }
    for (String segment : childs) {
      newSegments.add(SegmentInfo.retrieveFromZookeeper(zkClient, clusterName, segment));
    }
    segments = newSegments;
  }

  @Override
  protected void doStop() {
    // TODO Auto-generated method stub
    
  }

}
