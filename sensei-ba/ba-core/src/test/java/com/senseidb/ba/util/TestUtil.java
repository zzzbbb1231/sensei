package com.senseidb.ba.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import org.json.JSONObject;

import com.linkedin.gazelle.dao.GazelleIndexSegmentImpl;
import com.senseidb.ba.BASentinelTest;
import com.senseidb.ba.IndexSegmentImpl;
import com.senseidb.ba.index1.InMemoryAvroMapper;
import com.senseidb.ba.index1.SegmentPersistentManager;
import com.senseidb.ba.util.TarGzCompressionUtils;

public class TestUtil {

  public static GazelleIndexSegmentImpl createIndexSegment() throws URISyntaxException, Exception {
    File avroFile = new File(BASentinelTest.class.getClassLoader().getResource("data/sample_data.avro").toURI());
    GazelleIndexSegmentImpl indexSegmentImpl = new InMemoryAvroMapper(avroFile).build();
     return indexSegmentImpl;
  }

  public static File createCompressedSegment(String segmentId, GazelleIndexSegmentImpl indexSegmentImpl, File tempIndexDir) throws Exception {
    File segmentDir = new File(tempIndexDir, segmentId);
  
    
    SegmentPersistentManager.persist(segmentDir, indexSegmentImpl);
    File compressedFile = new File(tempIndexDir, segmentId + ".tar.gz");
    TarGzCompressionUtils.createTarGzOfDirectory(segmentDir.getAbsolutePath() + "/", compressedFile.getAbsolutePath());
    return compressedFile;
  }

  public static JSONObject search(URL url, String req) throws Exception {
    URLConnection conn = url.openConnection();
    conn.setDoOutput(true);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream(), "UTF-8"));
    String reqStr = req;
    System.out.println("req: " + reqStr);
    writer.write(reqStr, 0, reqStr.length());
    writer.flush();
    BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
    StringBuilder sb = new StringBuilder();
    String line = null;
    while((line = reader.readLine()) != null)
      sb.append(line);
    String res = sb.toString();
    // System.out.println("res: " + res);
    JSONObject ret = new JSONObject(res);
    if (ret.opt("totaldocs") !=null){
     // assertEquals(15000L, ret.getLong("totaldocs"));
    }
    return ret;
  }

}
