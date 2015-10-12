package org.apache.nifi.provenance;

import java.util.NoSuchElementException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * A non-blocking buffer that it will never block on the add(..) call 
 * essentially overflowing on itself by overriding values which may not 
 * have been retrieved yet. <br>
 * For example:<br>
 * Assume at certain state buffer looks like this - [1, 2, 3, 4]
 * The consuming thread managed to retrieve [1, 2]. And while processing [2]
 * the subscriber added [5, 6], making the buffer look like this [5, 6, 3, 4].
 * If consumer is capable to keep up, it will continue with [3, 4] and then [5, 6]. 
 * However if by the time consumer had finished processing [2], the subscriber 
 * added [7, 8], the next value seen by the consumer will be [5] and so on.
 * <br> 
 * This buffer implementation also ensures that newest events are not followed by 
 * older events. <br>
 * For example, assume before consumer was able to retrieve the first event the buffer 
 * state is [5, 6, 3, 4]. On the first attempt to access the buffer the consumer will 
 * get [3] and [4] followed by [5] and [6], thus ensuring the sequential ordering.
 *
 * @param <T>
 */
final class WeakReferenceRingBuffer<T> {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final T[] buffer;

	private final int mask;

	private volatile int writeCounter;

	private volatile int readCounter;

	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	
	private final Semaphore readPermits;

	/**
	 * 
	 * @param size
	 */
	@SuppressWarnings("unchecked")
	public WeakReferenceRingBuffer(int size) {
		if (!(size > 1 && ((size & (size - 1)) == 0))) {
			throw new IllegalArgumentException("'size' must be > 1 and power of 2; was " + size);
		}

		this.buffer = (T[]) new Object[size];
		this.mask = size - 1;
		this.readPermits = new Semaphore(size);
		this.readPermits.tryAcquire(size);
	}

	/**
	 * 
	 * @param object
	 */
	public void add(T reference) {
		try {
			this.lock.writeLock().lockInterruptibly();
			try {
				this.buffer[this.writeCounter++ & this.mask] = reference;
				this.readPermits.release();
			} catch (Exception e) {
				logger.error("Failure while adding value to buffer", e);
			} finally {
				this.lock.writeLock().unlock();
			}
		} catch (InterruptedException e) {
			logger.warn("Current thread is interrupted", e);
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Retrieves and removes the head of this queue. 
	 *
	 * @return the head of this queue
	 * @throws NoSuchElementException
	 *             if this queue is empty
	 */
	public T next() {
		T result = null;
		try {
			this.readPermits.acquire();
			this.lock.readLock().lockInterruptibly();
			try {
				if (this.readCounter < this.writeCounter) {
					int diff = this.writeCounter - this.readCounter;
					if (diff > this.buffer.length) {
						this.readCounter += (diff - this.buffer.length);
					}
					result = this.buffer[this.readCounter & this.mask];
				}
			} catch (Exception e) {
				logger.error("Failure while retrieving value from the buffer", e);
			} finally {
				this.lock.readLock().unlock();
			}
		} catch (InterruptedException e) {
			logger.warn("Current thread is interrupted", e);
			Thread.currentThread().interrupt();
		}

		if (result != null) {
			this.readCounter++;
		}

		return result;
	}
}
