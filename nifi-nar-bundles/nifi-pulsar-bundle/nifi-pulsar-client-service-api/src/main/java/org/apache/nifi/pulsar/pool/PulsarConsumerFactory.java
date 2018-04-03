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
package org.apache.nifi.pulsar.pool;

import java.util.Properties;

import org.apache.nifi.pulsar.PulsarConsumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarConsumerFactory implements ResourceFactory<PulsarConsumer> {

    public static final String TOPIC_NAME = "topic";
    public static final String SUBSCRIPTION_NAME = "subscription";
    public static final String CONSUMER_CONFIG = "Consumer-Configuration";

    private PulsarClient client;
    private String pulsarBrokerRootUrl;

    public PulsarConsumerFactory(PulsarClient client, String pulsarBrokerRootUrl) {
      this.client = client;
      this.pulsarBrokerRootUrl = pulsarBrokerRootUrl;
    }

    @Override
    public PulsarConsumer create(Properties props) throws ResourceCreationException {

      String topic = props.getProperty(TOPIC_NAME);
      String subscription = props.getProperty(SUBSCRIPTION_NAME);
      ConsumerConfiguration config = (ConsumerConfiguration) props.get(CONSUMER_CONFIG);

      try {
        // If we have a ProducerConfiguration then use it, otherwise a topic name will suffice
        return (config == null) ? new PulsarConsumer(client.subscribe(topic, subscription), pulsarBrokerRootUrl, topic, subscription) :
              new PulsarConsumer(client.subscribe(topic, subscription, config), pulsarBrokerRootUrl, topic, subscription);

       } catch (PulsarClientException e) {
         throw new ResourceCreationException(e);
       }

    }

}
