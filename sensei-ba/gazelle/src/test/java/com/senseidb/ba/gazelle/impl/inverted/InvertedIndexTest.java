package com.senseidb.ba.gazelle.impl.inverted;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.browseengine.bobo.facets.data.TermStringList;
import com.browseengine.bobo.facets.data.TermValueList;
import com.senseidb.ba.gazelle.ColumnMetadata;
import com.senseidb.ba.gazelle.SingleValueForwardIndex;
import com.senseidb.ba.gazelle.impl.GazelleForwardIndexImpl;
import com.senseidb.ba.gazelle.impl.GazelleInvertedIndexImpl;
import com.senseidb.ba.gazelle.utils.HeapCompressedIntArray;

public class InvertedIndexTest{
	private String column;
	private HeapCompressedIntArray data;
	private TermValueList<?> dictionary;
	private ColumnMetadata columnMetadata;

	private SingleValueForwardIndex fIndex;
	private GazelleInvertedIndexImpl iIndex;
	private DocIdSetIterator iIterator;

	private int TEST_SIZE = 10000000;

	@Before
	public void setUp() throws Exception {
		column = "test";

		data = new HeapCompressedIntArray(TEST_SIZE, 64);
		int[] data2 = {0,1,2,3,4,0,1,2,3,4,5,6,7,8,9};
		for(int i = 0; i < TEST_SIZE; i++){
			data.setInt(i, data2[i % 15]);
		}

		dictionary = new TermStringList(10);
		for(int i = 0; i < 10; i++){
			dictionary.add(Integer.toString(i));
		}

		columnMetadata = new ColumnMetadata();

		fIndex = new GazelleForwardIndexImpl(column, data, dictionary, columnMetadata);
		int optVal = GazelleInvertedIndexImpl.estimateOptimalMinJump(fIndex, dictionary.indexOf("0"));
		iIndex = new GazelleInvertedIndexImpl(fIndex, dictionary.indexOf("0"), optVal);
		for(int i = 0; i < TEST_SIZE; i++){
			if(fIndex.getReader().getValueIndex(i) == 0){
				iIndex.addDoc(i);
			}
		}
		iIterator = iIndex.iterator();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test1nextDocBasic() throws IOException {

		int sum = 0;
//		long start = 0, end = 0;
//
//		for(int k = 0; k < 10; k++){
//
			DocIdSetIterator iIndex2 = iIndex.iterator();
			int current = iIndex2.nextDoc();
			int i = 0;

//			start = System.currentTimeMillis();
			while(current != DocIdSetIterator.NO_MORE_DOCS){
				assertEquals(i, current, 0);
				current = iIndex2.nextDoc();
				i += 5;

				if(current == DocIdSetIterator.NO_MORE_DOCS)
					break;

				assertEquals(i, current, 0);
				current = iIndex2.nextDoc();
				i += 10;
			}
//			end = System.currentTimeMillis();
//			sum += (end-start);
//		}
//
//		System.out.println("The average time to iterate through all documents was: " + sum/10);
	}

	@Test
	public void test2advanceBasic() throws IOException {

		int sum = 0;
//		long start = 0, end = 0;
//
//		for(int k = 0; k < 10; k++){

			DocIdSetIterator iIndex2 = iIndex.iterator();

			int current = iIndex2.advance(0);
			int i = 0;
//			start = System.currentTimeMillis();
			while(current != DocIdSetIterator.NO_MORE_DOCS){
				assertEquals(i, current, 0);
				i += 5;
				current = iIndex2.advance(i);

				if(current == DocIdSetIterator.NO_MORE_DOCS)
					break;

				assertEquals(i, current, 0);
				i += 10;
				current = iIndex2.advance(i);
			}
//			end = System.currentTimeMillis();
//			sum += (end-start);
//		}
//
//		System.out.println("The average time to advance through all documents was: " + sum/10);
	}

	@Test
	public void test3nextDocadvanceBasic() throws IOException {

		int current = iIterator.nextDoc();
		int i = 0;

		while(current != DocIdSetIterator.NO_MORE_DOCS){
			assertEquals(i, current, 0);
			i += 5;
			current = iIterator.advance(i - 1);

			if(current == DocIdSetIterator.NO_MORE_DOCS)
				break;

			assertEquals(i, current, 0);
			i += 10;
			current = iIterator.nextDoc();
		}
	}

	@Test
	public void test4advancenextDocBasic() throws IOException {

		int current = iIterator.advance(0);
		int i = 0;

		while(current != DocIdSetIterator.NO_MORE_DOCS){
			assertEquals(i, current, 0);
			i += 5;
			current = iIterator.nextDoc();

			if(current == DocIdSetIterator.NO_MORE_DOCS)
				break;

			assertEquals(i, current, 0);
			i += 10;
			current = iIterator.advance(i);
		}
	}

	@Test
	public void test5advanceSkip() throws IOException {

		int current = iIterator.advance(0);
		int i = 0;

		while(current != DocIdSetIterator.NO_MORE_DOCS){
			assertEquals(i, current, 0);
			i += 2000;
			current = iIterator.advance(i);

			if(current == DocIdSetIterator.NO_MORE_DOCS)
				break;

			assertEquals(i, current, 0);
			i += 2005;
			current = iIterator.advance(i);
		}

	}

	@Test
	public void test6advanceBack() throws IOException {

		int current = iIterator.advance(0);
		int i = 0;

		while(current != DocIdSetIterator.NO_MORE_DOCS){
			assertEquals(i, current, 0);
			i += 5;
			current = iIterator.advance(0);

			if(current == DocIdSetIterator.NO_MORE_DOCS)
				break;

			assertEquals(i, current, 0);
			i += 10;
			current = iIterator.advance(0);
		}
	}

	@Test
	public void test7advanceForward() throws IOException {

		int current = iIterator.advance(0);
		int i = 0;

		while(current != DocIdSetIterator.NO_MORE_DOCS){
			assertEquals(i, current, 0);
			current = iIterator.advance(i + 6);
			i += 15;

			if(current == DocIdSetIterator.NO_MORE_DOCS)
				break;

			assertEquals(i, current, 0);
			current = iIterator.advance(i + 11);
			i += 15;
		}
	}

	@Test
	public void test8NoMoreDocs() throws IOException {

		iIterator.advance(TEST_SIZE);

		assertEquals(DocIdSetIterator.NO_MORE_DOCS, iIterator.nextDoc(), 0);
		assertEquals(DocIdSetIterator.NO_MORE_DOCS, iIterator.advance(0) , 0);

	}

	@Test
	public void test9advanceWithinJumps() throws IOException {

		int current = iIterator.advance(10);
		int i = 15;

		while(current != DocIdSetIterator.NO_MORE_DOCS){
			assertEquals(i, current, 0);
			i += 5;
			current = iIterator.nextDoc();

			if(current == DocIdSetIterator.NO_MORE_DOCS)
				break;

			assertEquals(i, current, 0);
			current = iIterator.advance(i + 5);
			i += 10;
		}		
	}

	@Test
	public void test10advanceBetweenJumps() throws IOException {

		int current = iIterator.advance(17);
		int i = 20;

		while(current != DocIdSetIterator.NO_MORE_DOCS){
			assertEquals(i, current, 0);
			i += 10;
			current = iIterator.nextDoc();

			if(current == DocIdSetIterator.NO_MORE_DOCS)
				break;

			assertEquals(i, current, 0);
			current = iIterator.advance(i + 3);
			i += 5;
		}		
	}

}