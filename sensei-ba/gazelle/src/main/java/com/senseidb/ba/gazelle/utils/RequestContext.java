package com.senseidb.ba.gazelle.utils;

public class RequestContext {
  
  public RequestContext(int chunkIndex, int skipListPosition, int bytePosition) {
    super();
    this.chunkIndex = chunkIndex;
    this.skipListPosition = skipListPosition;
    this.bytePosition = bytePosition;
  }
  int chunkIndex;
  int skipListPosition;
  int bytePosition;
  public int getChunkIndex() {
    return chunkIndex;
  }
  public int getBytePosition() {
    return bytePosition;
  }
  public int getSkipListPosition() {
    return skipListPosition;
  }
}
