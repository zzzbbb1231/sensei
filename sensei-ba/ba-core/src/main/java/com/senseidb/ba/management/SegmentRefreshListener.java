package com.senseidb.ba.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.gazelle.SegmentMetadata;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

public class SegmentRefreshListener implements IZkChildListener {
  private static Logger logger = Logger.getLogger(SegmentRefreshListener.class);  
  private static final Counter refreshedSegments = Metrics.newCounter(SegmentRefreshListener.class, "refreshedSegments");
  private static final Counter refreshEvents = Metrics.newCounter(SegmentRefreshListener.class, "refreshEvents");

  private SegmentTracker segmentTracker;
    private ZkClient zkClient;
    private final String clusterName;
    private String refreshMarkerPath;
    public SegmentRefreshListener(SegmentTracker segmentTracker, ZkClient zkClient, String clusterName) {
      super();
      this.segmentTracker = segmentTracker;
      this.zkClient = zkClient;
      this.clusterName = clusterName;
       refreshMarkerPath = SegmentUtils.getRefreshMarkerPath(clusterName);
      
    }
    private Set<String> currentRefreshMarkers = new HashSet<String>();
    public void start() {
      if (!zkClient.exists(refreshMarkerPath)) {
        try {
        zkClient.createPersistent(refreshMarkerPath, true);
        } catch (Exception ex) {
          logger.error(ex);
        }
      }
       zkClient.subscribeChildChanges(refreshMarkerPath, this);
      
    }
    public void stop() {
      zkClient.unsubscribeChildChanges(refreshMarkerPath, this);

    }
    @Override
    public synchronized void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
      List<String> toAddMarkers = new ArrayList<String>();
      if (currentChilds == null) {
        currentRefreshMarkers.clear();
        return;
      }
      refreshEvents.inc();
        for (String currentChild : currentChilds) {
          if (!currentRefreshMarkers.contains(currentChild) && currentChild != null) {
            List<String> segmentNames = zkClient.getChildren(SegmentUtils.getRefreshMarkerPath(clusterName) + "/" + currentChild);
            if (segmentNames != null && segmentNames.size() > 0) {
              toAddMarkers.addAll(segmentNames);
            }         
          }
        logger.info("Recieved an atomic refresh event for segments - " + toAddMarkers);
        currentRefreshMarkers.clear();
        currentRefreshMarkers.addAll(currentChilds);
      }
      List<String> availableSegments = new ArrayList<String>();
      synchronized(segmentTracker.getGlobalLock()) {
        availableSegments.addAll(segmentTracker.getSegmentsMap().keySet());
        availableSegments.addAll(segmentTracker.getLoadingSegments());
      }
      Set<String> segmentsToRefresh = new HashSet();
      for (String marker : toAddMarkers) {
        segmentsToRefresh.addAll(match(availableSegments, marker));
      }
      
      Map<String, String> existingCrcs = new HashMap<String, String>();
      synchronized(segmentTracker.getGlobalLock()) {
        for (String segment : segmentsToRefresh) {
          SegmentToZoieReaderAdapter segmentToZoieReaderAdapter = segmentTracker.getSegmentsMap().get(segment);
          if (segmentToZoieReaderAdapter != null) {
            GazelleIndexSegmentImpl gazelleIndexSegmentImpl = (GazelleIndexSegmentImpl) segmentToZoieReaderAdapter.getOfflineSegment();
            existingCrcs.put(segment, gazelleIndexSegmentImpl.getSegmentMetadata().getCrc());
          } else {
            existingCrcs.put(segment, null);
          }
        }
      }
      Map<String, GazelleIndexSegmentImpl> newSegmentsToRefresh = new HashMap<String, GazelleIndexSegmentImpl>();
      for (String segment : existingCrcs.keySet()) {
        String crc = SegmentUtils.getMetadata(zkClient, clusterName, segment).getProperty(SegmentMetadata.SEGMENT_CRC);
        if (crc != null && !crc.equals(existingCrcs.get(segment))) {
          SegmentInfo segmentInfo = SegmentInfo.retrieveFromZookeeper(zkClient, clusterName, segment);
          GazelleIndexSegmentImpl newSegment = segmentTracker.instantiateSegment(segment, segmentInfo);
          logger.warn("Failed to refresh a segment - " + segment);
          if (newSegment != null) {
            newSegmentsToRefresh.put(segment, newSegment);
          }
        }
      }
      if (newSegmentsToRefresh.size() == 0) {
        return;
      }
      refreshedSegments.inc(newSegmentsToRefresh.size());
      synchronized(segmentTracker.getGlobalLock()) {
      for  (String segment : newSegmentsToRefresh.keySet()) {
        if (segmentTracker.getSegmentsMap().containsKey(segment) || segmentTracker.getLoadingSegments().contains(segment)) {
          logger.info("Refresehed a segment - " + segment);
          segmentTracker.getSegmentsMap().put(segment, new SegmentToZoieReaderAdapter(newSegmentsToRefresh.get(segment), segment, segmentTracker.getSenseiDecorator()));
          segmentTracker.markSegmentAsLoaded(segment);
          
        }
      }
      }
    }
    private List<String> match(List<String> segments, String matchingSegment) {
      List<String> ret = new ArrayList<String>();
      if (!matchingSegment.contains("*")) {
         if (segments.contains(matchingSegment)) {
           ret.add(matchingSegment);
         }
         return ret;
      }
      
      String[] parts = matchingSegment.split("*");
       for (String segment : segments) {
         if (match(segment, parts)) {
           ret.add(segment);
         }
       }
       return ret;
    }
    /**
     * Performs wildcard matching
     * @param segment
     * @param parts
     * @return
     */
    private boolean match(String segment, String[] parts) {
      int startIndex = 0;
      
      for (String str : parts) {
        if (str == null || str.length() == 0) {
          continue;
        }
        if (startIndex >= segment.length()) {
          return false;
        }
        int nextIndex = segment.indexOf(str, startIndex);
        if (nextIndex < 0) {
          return false;
        }
        startIndex = nextIndex + str.length();
      } 
      return true;
    }
    
    
}
