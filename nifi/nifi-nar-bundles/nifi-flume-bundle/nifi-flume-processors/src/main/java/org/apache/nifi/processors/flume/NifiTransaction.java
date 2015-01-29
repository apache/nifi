
package org.apache.nifi.processors.flume;

import org.apache.flume.Event;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;


class NifiTransaction extends BasicTransactionSemantics {
  private final ProcessSession session;
  private final Relationship relationship;

  public NifiTransaction(ProcessSession session, Relationship relationship) {
    this.session = session;
    this.relationship = relationship;
  }

  @Override
  protected void doPut(Event event) throws InterruptedException {
    AbstractFlumeProcessor.transferEvent(event, session, relationship);
  }

  @Override
  protected Event doTake() throws InterruptedException {
    throw new UnsupportedOperationException("Only put supported");
  }

  @Override
  protected void doCommit() throws InterruptedException {
    session.commit();
  }

  @Override
  protected void doRollback() throws InterruptedException {
    session.rollback();
  }


}
