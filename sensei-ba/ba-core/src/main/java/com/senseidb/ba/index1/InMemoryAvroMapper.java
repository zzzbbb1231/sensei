package com.senseidb.ba.index1;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.browseengine.bobo.facets.data.TermValueList;
import com.linkedin.gazelle.creators.SegmentCreator;
import com.linkedin.gazelle.dao.GazelleIndexSegmentImpl;
import com.senseidb.ba.ColumnType;
import com.senseidb.ba.util.CompressedIntArray;

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
