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
package org.apache.nifi.pulsar;

import org.apache.nifi.pulsar.pool.PoolableResource;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarProducer implements PoolableResource {

    private static final Logger logger = LoggerFactory.getLogger(PulsarProducer.class);

    private final Producer producer;
    private boolean closed = false;
    private final String topicName;

    public PulsarProducer(Producer producer, String topicName) throws PulsarClientException {
       this.topicName = topicName;
       this.producer = producer;
    }

    public Producer getProducer() {
       return producer;
    }

    public String getName() {
      return topicName;
    }

    public boolean isClosed() {
       return this.closed;
    }

    public void close() {

      logger.info("Closing producer for topic {} ", new Object[] {topicName});

      this.closed = true;
      try {
         producer.close();
      } catch (PulsarClientException e) {
         logger.error("Unable to close connection to Pulsar due to {}; resources may not be cleaned up appropriately", e);
         closed = false;
      }

    }
}
