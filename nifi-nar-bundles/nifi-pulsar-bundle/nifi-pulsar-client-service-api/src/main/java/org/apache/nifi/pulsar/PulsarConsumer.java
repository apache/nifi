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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ConsumerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarConsumer implements PoolableResource {

    private static final Logger logger = LoggerFactory.getLogger(PulsarConsumer.class);

    private final Consumer consumer;
    private String pulsarBrokerRootUrl;
    private final String topicName;
    private final String subscriptionName;
    private boolean closed = false;

    public PulsarConsumer(Consumer consumer, String pulsarBrokerRootUrl, String topic, String subscription) throws PulsarClientException {
      this.pulsarBrokerRootUrl = pulsarBrokerRootUrl;
      this.consumer = consumer;
      this.topicName = topic;
      this.subscriptionName = subscription;
    }

    public void close() {

      logger.info("Closing consumer for topic {} and subscription {}", new Object[] {topicName, subscriptionName});
      closed = true;

      try {
        consumer.unsubscribe();
        consumer.close();
      } catch (PulsarClientException e) {
        logger.error("Unable to close connection to Pulsar due to {}; resources may not be cleaned up appropriately", e);
        closed = false;
      }
    }

    public boolean isClosed() {
      return closed;
    }

    public Consumer getConsumer() {
      return this.consumer;
    }

    public ConsumerStats getStats() {
      return this.consumer.getStats();
    }

    public String getTransitURL() {
       StringBuffer sb = new StringBuffer();
        sb.append(pulsarBrokerRootUrl).append("/")
          .append(topicName).append("/")
          .append(subscriptionName);

       return sb.toString();
    }
}
