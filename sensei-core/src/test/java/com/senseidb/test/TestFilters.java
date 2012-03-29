package com.senseidb.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import proj.zoie.api.ZoieSegmentReader;

import com.browseengine.bobo.api.BoboBrowser;
import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.api.BrowseRequest;
import com.browseengine.bobo.api.BrowseResult;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.data.TermListFactory;
import com.browseengine.bobo.facets.data.TermLongList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.browseengine.bobo.facets.impl.SimpleFacetHandler;
import com.senseidb.indexing.DefaultSenseiInterpreter;
import com.senseidb.search.filter.AscendingTimeFilter;

public class TestFilters extends TestCase {

  public TestFilters(){
    super();
  }
  
  public TestFilters(String name){
    super(name);
  }
  
  public void testAscTimeFilter() throws Exception{
    
    TermListFactory listFact = DefaultSenseiInterpreter.getTermListFactory(long.class);
    TermValueList valList = listFact.createTermList();
    // build a small sample index
    RAMDirectory ramDir = new RAMDirectory();
    IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_35, new StandardAnalyzer(Version.LUCENE_35));
    IndexWriter writer = new IndexWriter(ramDir,conf);
    for (int i=0;i<10;++i){
      Document doc = new Document();
      ZoieSegmentReader.fillDocumentID(doc, i);
      String val = valList.format(i);
      Field f = new Field("time",val,Store.NO,Index.NOT_ANALYZED_NO_NORMS);
      doc.add(f);
      writer.addDocument(doc);
    }
    writer.close();
    
    
    IndexReader reader = IndexReader.open(ramDir, true);
    SimpleFacetHandler timeFacet = new SimpleFacetHandler("time",listFact);
    List<FacetHandler<?>> list = new ArrayList<FacetHandler<?>>();
    list.add(timeFacet);
    BoboIndexReader boboReader = BoboIndexReader.getInstance(reader,list);
    
    BrowseRequest req = new BrowseRequest();
    AscendingTimeFilter timeFilter = new AscendingTimeFilter("time", TimeUnit.MILLISECONDS, 5);
    req.setFilter(timeFilter);
    req.setOffset(0);
    req.setCount(20);
    
    BoboBrowser browser = new BoboBrowser(boboReader);
    BrowseResult res = browser.browse(req);
    assertEquals(5, res.getNumHits());
    timeFilter = new AscendingTimeFilter("time", TimeUnit.MILLISECONDS, 0);
    req.setFilter(timeFilter);
    res = browser.browse(req);
    assertEquals(10, res.getNumHits());
    timeFilter = new AscendingTimeFilter("time", TimeUnit.MILLISECONDS, 10);
    req.setFilter(timeFilter);
    res = browser.browse(req);
    assertEquals(0, res.getNumHits());
    
    browser.close();
    reader.close();
  }
}
