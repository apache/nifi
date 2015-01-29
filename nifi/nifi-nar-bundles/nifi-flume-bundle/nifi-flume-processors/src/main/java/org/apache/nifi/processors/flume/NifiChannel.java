
package org.apache.nifi.processors.flume;

import org.apache.flume.Context;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;


public class NifiChannel extends BasicChannelSemantics {
  private final ProcessSession session;
  private final Relationship relationship;

  public NifiChannel(ProcessSession session, Relationship relationship) {
    this.session = session;
    this.relationship = relationship;
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new NifiTransaction(session, relationship);
  }

  @Override
  public void configure(Context context) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }


}
