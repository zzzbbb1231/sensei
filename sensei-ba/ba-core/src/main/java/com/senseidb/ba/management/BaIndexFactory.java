package com.senseidb.ba.management;

import java.io.File;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.management.StandardMBean;

import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.fs.FileSystem;
import org.apache.lucene.analysis.Analyzer;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.mbean.ZoieAdminMBean;

import com.browseengine.bobo.api.BoboIndexReader;
import com.senseidb.ba.gazelle.utils.ReadMode;
import com.senseidb.search.node.SenseiIndexReaderDecorator;

public class BaIndexFactory implements Zoie<BoboIndexReader, Object> {
	private final File idxDir;
	private final SenseiIndexReaderDecorator decorator;
	private final ZkClient zkClient;
	private final FileSystem fileSystem;
	private final int partitionId;
	private ZookeeperTracker zookeeperTracker;
	private SegmentTracker segmentTracker;
	private final ExecutorService executorService;
	private final String clusterName;
	private final ReadMode readMode;
	private final int nodeId;
	private String[] invertedColumns;

	public BaIndexFactory(File idxDir, String clusterName, SenseiIndexReaderDecorator decorator, ZkClient zkClient, FileSystem fileSystem, ReadMode readMode, int nodeId, int partitionId, ExecutorService executorService, String[] invertedColumns) {
		this(idxDir, clusterName, decorator, zkClient, fileSystem, readMode, partitionId, partitionId, executorService);
		this.invertedColumns = invertedColumns;
	}

	public BaIndexFactory(File idxDir, String clusterName, SenseiIndexReaderDecorator decorator, ZkClient zkClient, FileSystem fileSystem, ReadMode readMode, int nodeId, int partitionId, ExecutorService executorService) {
		this.idxDir = idxDir;
		this.clusterName = clusterName;
		this.decorator = decorator;
		this.zkClient = zkClient;
		this.fileSystem = fileSystem;
		this.readMode = readMode;
		this.nodeId = nodeId;
		this.partitionId = partitionId;
		this.executorService = executorService != null ? executorService : Executors.newSingleThreadExecutor();

	}



	@Override
	public void consume(Collection<proj.zoie.api.DataConsumer.DataEvent<Object>> data) throws ZoieException {
		throw new UnsupportedOperationException();

	}

	@Override
	public String getVersion() {
		return null;
	}

	@Override
	public Comparator<String> getVersionComparator() {
		return null;
	}

	@Override
	public List<ZoieIndexReader<BoboIndexReader>> getIndexReaders()  {
		return segmentTracker.getIndexReaders();
	}

	@Override
	public Analyzer getAnalyzer() {
		return null;
	}

	@Override
	public void returnIndexReaders(List<ZoieIndexReader<BoboIndexReader>> r) {
		segmentTracker.returnIndexReaders(r);

	}

	@Override
	public String getCurrentReaderVersion() {
		return null;
	}

	@Override
	public void start() {
		if (!idxDir.exists()) {
			idxDir.mkdirs();
		}
		segmentTracker = new SegmentTracker();
		segmentTracker.start(idxDir, fileSystem, decorator, readMode, executorService, invertedColumns);
		zookeeperTracker = new ZookeeperTracker(zkClient, clusterName, nodeId, partitionId, segmentTracker);
		zookeeperTracker.start();

	}

	@Override
	public void shutdown() {
		zookeeperTracker.stop();
		segmentTracker.stop();
	}

	@Override
	public StandardMBean getStandardMBean(String name) {
		return null;
	}

	@Override
	public String[] getStandardMBeanNames() {
		// TODO Auto-generated method stub
		return new String[0];
	}

	@Override
	public ZoieAdminMBean getAdminMBean() {
		return null;
	}

	@Override
	public void syncWithVersion(long timeInMillis, String version) throws ZoieException {

	}

	@Override
	public void flushEvents(long timeout) throws ZoieException {

	}



	public SegmentTracker getSegmentTracker() {
		return segmentTracker;
	}

}