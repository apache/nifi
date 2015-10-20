package org.apache.nifi.provenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class WeakReferenceRingBufferTests {

	@Test(expected=IllegalArgumentException.class)
	public void testBoundGreaterThenOneAndPowerOfTwo(){
		new WeakReferenceRingBuffer<>(3);
	}
	
	@Test
	public void testWillNotBlockWhenOverCapacity(){
		WeakReferenceRingBuffer<Integer> queue = new WeakReferenceRingBuffer<>(2);
		for (int i = 0; i < 10; i++) {
			queue.add(i);
		}
		// last two should be 8, 9 and then null
		assertEquals(new Integer(8), queue.next());
		assertEquals(new Integer(9), queue.next());
		assertNull(queue.next());
	}
	
	@Test
	public void testSequenceOrderingAfterOverflow(){
		WeakReferenceRingBuffer<Integer> queue = new WeakReferenceRingBuffer<>(4);
		for (int i = 0; i < 6; i++) {
			queue.add(i);
		}
		
		assertEquals(new Integer(2), queue.next());
		assertEquals(new Integer(3), queue.next());
		assertEquals(new Integer(4), queue.next());
		assertEquals(new Integer(5), queue.next());
		assertNull(queue.next());
	}
	
	@Test
	public void testNoDuplicatesWithAsyncRetrievalSmallBuffer() throws Exception {
		this.testNoDuplicatesWithAsyncRetrieval(256);
	}
	
	@Test
	public void testNoDuplicatesWithAsyncRetrievalLargeBuffer() throws Exception {
		this.testNoDuplicatesWithAsyncRetrieval(524288);
	}
	
	public void testNoDuplicatesWithAsyncRetrieval(int bufferSize) throws Exception {
		final WeakReferenceRingBuffer<Integer> queue = new WeakReferenceRingBuffer<>(262144);
		final ArrayList<Integer> values = new ArrayList<>();
		ExecutorService exe = Executors.newSingleThreadExecutor();
		final AtomicBoolean continueRead = new AtomicBoolean(true);
		exe.execute(new Runnable() {
			@Override
			public void run() {
				while (continueRead.get()){
					Integer value = queue.next();
					if (value != null){
						values.add(value);
					}
				}
			}
		});
		int eventsToProduce = 3000000;
		long start = System.currentTimeMillis();
		for (int i = 0; i < eventsToProduce; i++) {
			queue.add(i);
		}
		long stop = System.currentTimeMillis();
		System.out.println("Added " + eventsToProduce + " events in " + (stop-start) + " millis");
		continueRead.set(false);
		exe.shutdown();
		if (exe.awaitTermination(1000, TimeUnit.MILLISECONDS)){
			int lastValue = -1;
			for (Integer integer : values) {
				if (integer > lastValue){
					lastValue = integer;
				}
				else {
					fail();
				}
			}
		}
		else {
			fail();
		}
	}
}
