package org.apache.nifi.remote.protocol;

import org.apache.nifi.remote.Peer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class FlowFileRequest {
  private final Peer peer;
  private final ServerProtocol protocol;
  private final BlockingQueue<ProcessingResult> queue;
  private final long creationTime;
  private final AtomicBoolean beingServiced = new AtomicBoolean(false);

  public FlowFileRequest(final Peer peer, final ServerProtocol protocol) {
    this.creationTime = System.currentTimeMillis();
    this.peer = peer;
    this.protocol = protocol;
    this.queue = new ArrayBlockingQueue<>(1);
  }

  public void setServiceBegin() {
    this.beingServiced.set(true);
  }

  public boolean isBeingServiced() {
    return beingServiced.get();
  }

  public BlockingQueue<ProcessingResult> getResponseQueue() {
    return queue;
  }

  public Peer getPeer() {
    return peer;
  }

  public ServerProtocol getProtocol() {
    return protocol;
  }

  public boolean isExpired() {
    // use double the protocol's expiration because the sender may send data for a bit before
    // the timeout starts being counted, and we don't want to timeout before the sender does.
    // is this a good idea...???
    long expiration = protocol.getRequestExpiration() * 2;
    if (expiration <= 0L) {
      return false;
    }

    if (expiration < 500L) {
      expiration = 500L;
    }

    return System.currentTimeMillis() > creationTime + expiration;
  }
}
