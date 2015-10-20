package org.apache.nifi.provenance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic subscriber which implements a direct-handoff message
 * passing interface (MPI) by asynchronously handing off events received from
 * the event stream to a provided consumer.<br>
 * <br>
 * Unlike conventional direct-handoff MPI, this implementation will never
 * block while receiving events, since it relies on {@link WeakReferenceRingBuffer} which 
 * will overflow onto itself in cases where consumer is not able to keep up with the 
 * rate of incoming events resulting in event loss. This means that event streaming 
 * should be used for specialized use cases only where event loss is either acceptable 
 * or consumer implements appropriate safe-guards to ensure that buffer doesn't overflow.
 * <br>
 * While this implementation does not prevent event loss it does provide safe-guards
 * to ensure sequential correctness and no-duplicates. See {@link WeakReferenceRingBuffer} 
 * documentation for more details.
 *
 */
final class ProvenanceEventSubscriber {
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final ExecutorService executor;

	private final WeakReferenceRingBuffer<ProvenanceEventRecord> eventBuffer;

	private final ProvenanceEventConsumer consumer;
	
	private volatile boolean running;

	/**
	 * Constructs an instance of the subscriber with a provided {@link ProvenanceEventConsumer}.
	 */
	ProvenanceEventSubscriber(ProvenanceEventConsumer consumer) {
		this.executor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1));
		this.consumer = consumer;
		this.eventBuffer = new WeakReferenceRingBuffer<>(1024);
	}
	
	/**
	 * Starts this subscriber by setting its state to running and initiating 
	 * consumer task.
	 */
	void start(){
		if (!this.running){
			this.running = true;
			this.executor.execute(new Runnable() {	
				@Override
				public void run() {
					try {
						while (running){
							consumer.consume(eventBuffer.next());
						}
					} catch (Exception e) {
						logger.error("Consumer threw exception, subscriber will be stopped", e);
						asyncStop();
					}
				}
			});
		} else {
			logger.warn("Subscriber is already running");
		}
	}
	
	/**
	 * Will stop this subscriber. Subsequent calls to this method 
	 * will have no effect if this subscriber is already stopped.
	 */
	void stop(){
		if (this.running){
			this.running = false;
			this.executor.shutdown();
			try {
				this.executor.awaitTermination(100, TimeUnit.MILLISECONDS);
			} 
			catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				logger.warn("Interrupted while shutting down executor");
			}
		}
	}
	
	/**
	 * 
	 * @return
	 */
	boolean isStopped(){
		return !this.running;
	}
	
	/**
	 * Receives events from the event stream and places them on the event buffer.
	 */
	final void receive(ProvenanceEventRecord event) {
		try {
			if (this.running){
				this.eventBuffer.add(event);
			}
			else {
				logger.warn("Subscriber is not started");
			}
		} catch (Exception e) {
			// realistically should never happen, yet if it does must not shut down NiFi 
			logger.error("Subscriber threw exception and will be stopped.", e);
			this.asyncStop();
		}
	}
	
	/**
	 * 
	 */
	private void asyncStop(){
		new Thread(new Runnable() {
			@Override
			public void run() {
				stop();
			}
		}).start();
	}
}
