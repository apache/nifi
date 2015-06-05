/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
