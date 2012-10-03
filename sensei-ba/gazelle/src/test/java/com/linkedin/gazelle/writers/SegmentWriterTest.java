package com.linkedin.gazelle.writers;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.linkedin.gazelle.creators.DictionaryCreator;
import com.linkedin.gazelle.creators.SegmentCreator;


public class SegmentWriterTest {
  private DictionaryCreator _dictionaryWriter;
  private File _indexDir;
  private File _avroFile;

  @Before
  public void setup() throws IOException {
    _indexDir = new File("index");
    _indexDir.delete();
    _indexDir.mkdir();

    String avroFilepath = System.getProperty("user.dir")
        + "/src/test/resources/data/sample_data.avro";
    _avroFile = new File(avroFilepath);
  }

  @Test
  public void validate() {
    SegmentCreator writer = new SegmentCreator();
   // writer.process(_avroFile);
   
  }
}
