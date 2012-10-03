package com.senseidb.ba.index1;

import java.io.File;

import com.senseidb.ba.gazelle.creators.SegmentCreator;
import com.senseidb.ba.gazelle.dao.GazelleIndexSegmentImpl;

public class InMemoryAvroMapper  {
	private long startOffset = 0;
  private final File avroFile;
	public InMemoryAvroMapper(File avroFile) {
    this.avroFile = avroFile;
		
		
	}
	public GazelleIndexSegmentImpl build() throws Exception {
    return new SegmentCreator().process(avroFile);
 }

}
