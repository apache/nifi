package org.apache.nifi.provenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.junit.Test;
import org.mockito.Mockito;

public class ProvenanceEventSubscriberTests {
	
	private final AtomicInteger counter = new AtomicInteger();
	
	private final Random random = new Random();

	
	@Test
	/**
	 * This test validates that events are not distributed to the consumer if subscriber is 
	 * not started or stopped.
	 * It also validates that while subscriber may end up to be in invalid state (not started or stopped) 
	 * it will not bring NiFi to its knees.
	 */
	public void testNonStartedOrSoppedSubscriberWontThrowException() throws Exception {
		ProvenanceEventSubscriber subscriber = new ProvenanceEventSubscriber(new TestFastConsumer());
		ProvenanceEventRecord event = Mockito.mock(ProvenanceEventRecord.class);
		for (int i = 0; i < 4; i++) {
			subscriber.receive(event);
		}
		Thread.sleep(10); // give some time for consumer to process what's there for testing purposes
		assertEquals(0, counter.get());
		subscriber.stop();
		for (int i = 0; i < 4; i++) {
			subscriber.receive(event);
		}
		assertEquals(0, counter.get());
	}
	
	@Test
	public void testAllEventsConsumedIfLessThenBufferSize() throws Exception {// 1024 buffer size
		ProvenanceEventSubscriber subscriber = new ProvenanceEventSubscriber(new TestFastConsumer());
		subscriber.start();
		ProvenanceEventRecord event = Mockito.mock(ProvenanceEventRecord.class);
		for (int i = 0; i < 1024; i++) {
			subscriber.receive(event);
		}
		Thread.sleep(10); // give some time for consumer to process what's there for testing purposes
		assertEquals(1024, counter.get());
	}
	
	@Test
	/**
	 * Validates that any exception thrown by the consumer will simply stop the subscriber 
	 * without halting the NiFi. Preferably, consumer exceptions have to be handled within
	 * the consumers themselves. 
	 */
	public void testExceptionThrowingConsumer() throws Exception {
		ProvenanceEventSubscriber subscriber = new ProvenanceEventSubscriber(new TestExceptionThrowingConsumer());
		subscriber.start();
		ProvenanceEventRecord event = Mockito.mock(ProvenanceEventRecord.class);
		for (int i = 0; i < 20; i++) {
			subscriber.receive(event);
		}
		Thread.sleep(10); // give some time for consumer to process what's there for testing purposes
		assertEquals(11, counter.get());
		assertTrue(subscriber.isStopped());
	}
	
	@Test
	/**
	 * Should never happen, but still worth checking
	 */
	public void testExceptionThrowingSubscriber() throws Exception {
		ProvenanceEventSubscriber subscriber = new ProvenanceEventSubscriber(new TestFastConsumer());
		Field eventBufferField = ProvenanceEventSubscriber.class.getDeclaredField("eventBuffer");
		eventBufferField.setAccessible(true);
		subscriber.start();
		ProvenanceEventRecord event = Mockito.mock(ProvenanceEventRecord.class);
		for (int i = 0; i < 20; i++) {
			subscriber.receive(event);
			if (i == 10){
				eventBufferField.set(subscriber, null);// will force NPE on the next iteration
			}
		}
		Thread.sleep(10); // give some time for consumer to process what's there for testing purposes
		assertEquals(11, counter.get());
		assertTrue(subscriber.isStopped());
	}
	
	@Test
	/**
	 * This test demonstrates the ability of the subscriber to distribute ~2M events in under 1 second
	 * to the consumer which can also process them (providing relatively simple processing logic)
	 * without any event loss (an equally fast consumer). 
	 * 
	 * This test may not perform if executed on ancient hardware.
	 */
	public void testHighThroughputWithDefaultBufferSize() throws Exception {
		ProvenanceEventSubscriber subscriber = new ProvenanceEventSubscriber(new TestFastConsumer());
		subscriber.start();
		ProvenanceEventRecord event = Mockito.mock(ProvenanceEventRecord.class);
		int eventsToConsume = 2000000;
		long start = System.currentTimeMillis();
		for (int i = 0; i < eventsToConsume; i++) {
			subscriber.receive(event);
		}
		long stop = System.currentTimeMillis();
		System.out.println("Distributed " + eventsToConsume + " in " + (stop-start) + " millis");
		assertTrue((stop-start) < 1000);
		Thread.sleep(100);
		assertEquals(eventsToConsume, counter.get());
	}
	
	@Test
	/**
	 * This test is primarily to validate that the speed of event distribution will
	 * not be affected by the speed of event consumption.
	 */
	public void testSlowConsumer() throws Exception {
		ProvenanceEventSubscriber subscriber = new ProvenanceEventSubscriber(new TestSlowConsumer());
		subscriber.start();
		ProvenanceEventRecord event = Mockito.mock(ProvenanceEventRecord.class);
		int eventsToConsume = 2000000;
		long start = System.currentTimeMillis();
		for (int i = 0; i < eventsToConsume; i++) {
			subscriber.receive(event);
		}
		long stop = System.currentTimeMillis();
		System.out.println("Distributed " + eventsToConsume + " in " + (stop-start) + " millis");
		assertTrue((stop-start) < 1000);
		Thread.sleep(100);
		assertTrue(counter.get() < eventsToConsume); // events were lost
	}
	
	private class TestFastConsumer implements ProvenanceEventConsumer {
		@Override
		public void consume(ProvenanceEventRecord event) {
			counter.incrementAndGet();
		}
	}
	
	private class TestSlowConsumer implements ProvenanceEventConsumer {
		@Override
		public void consume(ProvenanceEventRecord event) {
			counter.incrementAndGet();
			LockSupport.parkNanos(random.nextInt(100));
		}
	}
	
	private class TestExceptionThrowingConsumer implements ProvenanceEventConsumer {
		@Override
		public void consume(ProvenanceEventRecord event) {
			counter.incrementAndGet();
			if (counter.get() > 10){
				throw new RuntimeException("Intentional for testing");
			}
		}
	}
}
