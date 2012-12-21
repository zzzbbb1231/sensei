package com.senseidb.ba.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import scala.actors.threadpool.Arrays;

import com.senseidb.ba.format.GenericIndexCreator;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.gazelle.persist.SegmentPersistentManager;
import com.senseidb.ba.management.SegmentTracker;


public class IndexConverter {
  private static Logger logger = Logger.getLogger(IndexConverter.class);
  public static void main(String[] args) throws Exception{
    System.out.println(Arrays.toString(args));
    String usage = "Usage:\n" + 
                    "<segmentname> <file path>  [--exclude=<<comma separated list of excluded columns>>] [--sort=<<comma separated list of  columns to sort>>] [output directory]";
    if (args.length < 2) {
      System.out.println(usage);
      System.exit(0);
    }
       String segmentName = args[0];
       File file = new File(args[1]);     
       String excluded = null;
       String sorted = null;
      int count = 0;
       for (int i = 2; i < args.length; i++) {
        if (args[i].startsWith("--exclude=")) {
          excluded = args[i].substring("--exclude=".length());
          count++;          
        }
        if (args[i].startsWith("--sort=")) {
          sorted = args[i].substring("--sort=".length());
          count++;
        }
      }
      List<String> excludedColumns = split(excluded);
      List<String> sortedColumns = split(sorted);
      File dir = null;
      if (args.length > 2 + count) {
        dir = new File(args[2 + count]);
      } else {
        dir = new File("");
      }
      dir.mkdirs();
      GazelleIndexSegmentImpl created = GenericIndexCreator.create(file, sortedColumns.toArray(new String[sortedColumns.size()]),  excludedColumns.toArray(new String[excludedColumns.size()]));
      logger.info("tarring the segment");
      File segmentDir = new File(dir, segmentName);
      SegmentPersistentManager.flushToDisk(created, segmentDir);
      TarGzCompressionUtils.createTarGzOfDirectory(segmentDir.getAbsolutePath(), new File(dir, segmentName + ".tar.gz").getAbsolutePath());
      logger.info("The segment was created");
    }

  private static List<String> split(String str) {
    List<String> ret = new ArrayList<String>();
    if (str == null) {
      return ret;
    }
    for (String part : str.split(",")) {
      part = part.trim();
      if (part.length() > 0) {
        ret.add(part);
      }
    }
    return ret;
  }
}
