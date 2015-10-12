package org.apache.nifi.provenance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base implementation of ProvenanceEventRepository.
 * Handles common operations
 */
public abstract class AbstractProvenanceRepository implements ProvenanceEventRepository {
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	
	private final List<ProvenanceEventSubscriber> eventSubscribers;
	
	/**
	 * 
	 */
	public AbstractProvenanceRepository(){
		List<ProvenanceEventConsumer> eventConsumers = this.discoverAndLoadConsumers();
		if (eventConsumers != null){
			this.eventSubscribers = new ArrayList<>();	
			for (ProvenanceEventConsumer provenanceEventConsumer : eventConsumers) {
				ProvenanceEventSubscriber eventSubscriber = new ProvenanceEventSubscriber(provenanceEventConsumer);
				eventSubscriber.start();
				this.eventSubscribers.add(eventSubscriber);
			}
		}
		else {
			// no need to create one if there are no consumers
			this.eventSubscribers = null;
		}
	}

	/**
	 * 
	 */
	@Override
	public void registerEvent(ProvenanceEventRecord event) {
		if (this.eventSubscribers != null){
			for (ProvenanceEventSubscriber eventSubscriber : this.eventSubscribers) {
				eventSubscriber.receive(event);
			}
		}
		this.doRegisterEvent(event);
	}
	
	@Override
	public void registerEvents(Iterable<ProvenanceEventRecord> events) {
		if (this.eventSubscribers != null){
			events = this.toReusableIterable(events);
			for (ProvenanceEventSubscriber eventSubscriber : this.eventSubscribers) {
				for (ProvenanceEventRecord event : events) {
					eventSubscriber.receive(event);
				}
			}
		}
		/*
		 * This is primarily to support current implementation of PersistentProvenanceRepository
		 * which calls private persistRecord(..) which accepts collection, so delegating
		 * to doRegisterEvent(singular) wouldn't be efficient since it creates a single entry 
		 * collection for each event.
		 */
		this.doRegisterEvents(events);
	}
	
	/**
	 * 
	 */
	public void close() throws IOException {
		if (this.eventSubscribers != null){
			for (ProvenanceEventSubscriber eventSubscriber : this.eventSubscribers) {
				eventSubscriber.stop();
			}
		}
		
		this.doClose();
	}
	
	/**
	 * 
	 */
	protected abstract void doRegisterEvent(ProvenanceEventRecord event);
	
	/**
	 * 
	 */
	protected abstract void doRegisterEvents(Iterable<ProvenanceEventRecord> events);
	
	/**
	 * 
	 */
	protected abstract void doClose() throws IOException;
	
	/**
	 * Will discover all available {@link ProvenanceEventConsumer}s
	 */
	private List<ProvenanceEventConsumer> discoverAndLoadConsumers(){
		List<ProvenanceEventConsumer> eventConsumers = null;
		Iterator<ProvenanceEventConsumer> sl = ServiceLoader
	            .load(ProvenanceEventConsumer.class, ClassLoader.getSystemClassLoader()).iterator();
		if (sl.hasNext()){
			eventConsumers = new ArrayList<>();
			while (sl.hasNext()){
				ProvenanceEventConsumer consumer = sl.next();
				logger.info("Registering provenance event consumer - " + consumer);
				eventConsumers.add(consumer);
			}
		}
		return eventConsumers;
	}
	
	/**
	 * Will produce an {@link Iterable} that can be iterated more then once.
	 */
	private Iterable<ProvenanceEventRecord> toReusableIterable(Iterable<ProvenanceEventRecord> events) {
		List<ProvenanceEventRecord> eventList = new ArrayList<>();
		for (ProvenanceEventRecord provenanceEventRecord : events) {
			eventList.add(provenanceEventRecord);
		}
		return eventList;
	}
}
