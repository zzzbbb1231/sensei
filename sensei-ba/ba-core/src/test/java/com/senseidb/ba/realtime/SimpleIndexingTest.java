package com.senseidb.ba.realtime;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.BASegmentRecoverTest;
import com.senseidb.ba.SegmentToZoieReaderAdapter;
import com.senseidb.ba.gazelle.IndexSegment;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleIndexSegmentImpl;
import com.senseidb.ba.realtime.domain.RealtimeSnapshotIndexSegment;
import com.senseidb.ba.realtime.domain.SingleValueSearchSnapshot;
import com.senseidb.ba.realtime.domain.primitives.dictionaries.LongDictionarySnapshot;
import com.senseidb.ba.realtime.domain.primitives.dictionaries.StringDictionarySnapshot;
import com.senseidb.ba.realtime.indexing.IndexingCoordinator;
import com.senseidb.ba.realtime.indexing.RealtimeIndexFactory;
import com.senseidb.ba.util.Wait;
import com.senseidb.plugin.SenseiPluginRegistry;

public class SimpleIndexingTest extends TestCase {
private SenseiPluginRegistry senseiPluginRegistry;
private IndexingCoordinator indexingCoordinator;

@Override
protected void setUp() throws Exception {
  File ConfDir1 = new File(BASegmentRecoverTest.class.getClassLoader().getResource("simple-indexing").toURI());
  String indexingDirectory = "baRealtimeIndexing";
  FileUtils.deleteDirectory(new File(indexingDirectory));
  PropertiesConfiguration conf = new PropertiesConfiguration();
  conf.setDelimiterParsingDisabled(true);
  conf.load(new File(ConfDir1, "sensei.properties"));
   senseiPluginRegistry = SenseiPluginRegistry.build(conf);
    indexingCoordinator = senseiPluginRegistry.getBeanByName("indexingCoordinator", IndexingCoordinator.class);
   indexingCoordinator.start();
}
@Override
  protected void tearDown() throws Exception {
  senseiPluginRegistry.stop();
  }
public void test1() throws Exception  {
  
 
  final Zoie zoieInstance = indexingCoordinator.getZoieInstance(1, 0);
  new Wait(5000) {
    
    @Override
    public boolean until() throws Exception {
      List indexReaders = zoieInstance.getIndexReaders();
      try {
      return indexReaders.size() == 1;
      } finally {
        zoieInstance.returnIndexReaders(indexReaders);
      }
    }
  };
  SegmentToZoieReaderAdapter segment = (SegmentToZoieReaderAdapter) zoieInstance.getIndexReaders().get(0);
  IndexSegment offlineSegment = segment.getOfflineSegment();
  assertEquals(100000, offlineSegment.getLength());
  final RealtimeIndexFactory realtimeInstance = (RealtimeIndexFactory) indexingCoordinator.getZoieInstance(0, 2);
  new Wait(500000000) {
    
    @Override
    public boolean until() throws Exception {
      
      SegmentToZoieReaderAdapter segmentToZoieReaderAdapter = (SegmentToZoieReaderAdapter)realtimeInstance.getIndexReaders().get(0);
      try {
      RealtimeSnapshotIndexSegment realtimeSnapshotIndexSegment = (RealtimeSnapshotIndexSegment) segmentToZoieReaderAdapter.getOfflineSegment();
      return realtimeSnapshotIndexSegment.getLength() >= 99999;
      } finally {
        realtimeInstance.returnIndexReaders(Arrays.asList((ZoieIndexReader<BoboIndexReader>)segmentToZoieReaderAdapter));
      }
    }
  };
  SegmentToZoieReaderAdapter segmentToZoieReaderAdapter = (SegmentToZoieReaderAdapter)realtimeInstance.getIndexReaders().get(0);
  RealtimeSnapshotIndexSegment realtimeSnapshotIndexSegment = (RealtimeSnapshotIndexSegment) segmentToZoieReaderAdapter.getOfflineSegment();
  
  assertEquals(99999, realtimeSnapshotIndexSegment.getLength());
  SingleValueSearchSnapshot idIndex = (SingleValueSearchSnapshot) realtimeSnapshotIndexSegment.getForwardIndex("id");
  LongDictionarySnapshot dictionarySnapshot = (LongDictionarySnapshot) idIndex.getDictionarySnapshot();
  assertEquals( 1000000 - 100000 ,dictionarySnapshot.getLongValue(idIndex.getForwardIndex()[0]));
  assertEquals( 1000000 - 100001 ,dictionarySnapshot.getLongValue(idIndex.getForwardIndex()[1]));
  
  int indexOf = dictionarySnapshot.sortedIndexOf(String.valueOf(1000000 - 100001));
  assertEquals(1000000 - 100001, dictionarySnapshot.getLongValue(dictionarySnapshot.getDictPermutationArray().getInt(indexOf)));
   indexOf = dictionarySnapshot.sortedIndexOf(String.valueOf(1000000 - 100000 - 99998));
  assertEquals(1000000 - 100000 - 99998, dictionarySnapshot.getLongValue(dictionarySnapshot.getDictPermutationArray().getInt(indexOf)));
  
  assertEquals( 1000000 - 100000 - 99998 ,dictionarySnapshot.getLongValue(idIndex.getForwardIndex()[99998]));
  SingleValueSearchSnapshot groupIndex = (SingleValueSearchSnapshot) realtimeSnapshotIndexSegment.getForwardIndex("groupId");
  StringDictionarySnapshot strDictionarySnapshot = (StringDictionarySnapshot) groupIndex.getDictionarySnapshot();
  
  indexOf = strDictionarySnapshot.sortedIndexOf(String.valueOf((1000000 - 100000) / 10));
  assertEquals(String.valueOf((1000000 - 100000) / 10), strDictionarySnapshot.getObject(strDictionarySnapshot.getDictPermutationArray().getInt(indexOf)));
 
  
  assertEquals( (1000000 - 100000) / 10 ,strDictionarySnapshot.getLongValue(groupIndex.getForwardIndex()[0]));
  assertEquals( (1000000 - 100001) / 10 ,strDictionarySnapshot.getLongValue(groupIndex.getForwardIndex()[1]));
  assertEquals( (1000000 - 100000 - 99998) / 10 ,strDictionarySnapshot.getLongValue(groupIndex.getForwardIndex()[99998]));
}

public void test2() throws Exception  {
  
  
  final Zoie zoieInstance = indexingCoordinator.getZoieInstance(1, 0);
  new Wait(5000) {
    
    @Override
    public boolean until() throws Exception {
      List indexReaders = zoieInstance.getIndexReaders();
      try {
      return indexReaders.size() == 1;
      } finally {
        zoieInstance.returnIndexReaders(indexReaders);
      }
    }
  };
  SegmentToZoieReaderAdapter segment = (SegmentToZoieReaderAdapter) zoieInstance.getIndexReaders().get(0);
  GazelleIndexSegmentImpl offlineSegment = (GazelleIndexSegmentImpl) segment.getOfflineSegment();
  assertEquals(100000, offlineSegment.getLength()); 
  SingleValueForwardIndex forwardIndex = (SingleValueForwardIndex) offlineSegment.getForwardIndex("id");
  assertEquals( Long.valueOf(1000000 - 99999) , (Long)forwardIndex.getDictionary().getRawValue(forwardIndex.getReader().getValueIndex(0)));
  assertEquals( Long.valueOf(1000000 - 99999 + 1) , (Long)forwardIndex.getDictionary().getRawValue(forwardIndex.getReader().getValueIndex(1)));
  assertEquals( Long.valueOf(1000000 - 99999 + 2) , (Long)forwardIndex.getDictionary().getRawValue(forwardIndex.getReader().getValueIndex(2)));
  assertEquals( Long.valueOf(1000000 - 99999 + 99999) , (Long)forwardIndex.getDictionary().getRawValue(forwardIndex.getReader().getValueIndex(99999)));
}
}
