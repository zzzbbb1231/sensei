package com.linkedin.gazelle.writers;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;


public class SegmentWriterTest {
  private DictionaryWriter _dictionaryWriter;
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
    SegmentWriter writer = new SegmentWriter(_avroFile);
    writer.process();
    writer.flushTo(_indexDir);
  }
}
