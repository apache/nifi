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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.flume.util.FlowFileEvent;

import java.util.ArrayList;
import java.util.List;


class NifiSinkTransaction extends BasicTransactionSemantics {
  private final ProcessSession session;
  private final Relationship success;
  private final Relationship failure;
  private final List<FlowFile> flowFiles;

  public NifiSinkTransaction(ProcessSession session, Relationship success, Relationship failure) {
    this.session = session;
    this.success = success;
    this.failure = failure;
    this.flowFiles = new ArrayList<>();
  }

  @Override
  protected void doPut(Event event) throws InterruptedException {
    AbstractFlumeProcessor.transferEvent(event, session, success);
  }

  @Override
  protected Event doTake() throws InterruptedException {
      FlowFile flowFile = session.get();
      if (flowFile == null) {
          return null;
      }
      flowFiles.add(flowFile);

      return new FlowFileEvent(flowFile, session);
  }

  @Override
  protected void doCommit() throws InterruptedException {
      session.transfer(flowFiles, success);
      session.commitAsync();
  }

  @Override
  protected void doRollback() throws InterruptedException {
      session.transfer(flowFiles, failure);
      session.commitAsync();
  }


}
