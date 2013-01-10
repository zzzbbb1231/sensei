package com.senseidb.ba.management;

public interface SegmentLoaderListener {
 public void segmentLoadedSuccsfully(String segmentId);
 public void segmentFailedToLoad(String segmentId);
}
