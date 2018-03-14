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

import org.apache.nifi.pulsar.PulsarProducer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;

public class PulsarProducerFactory implements ResourceFactory<PulsarProducer> {

    public static final String TOPIC_NAME = "topic";
    public static final String PRODUCER_CONFIG = "Producer-Configuration";

    private PulsarClient client;

    public PulsarProducerFactory(PulsarClient client) {
      this.client = client;
    }

    @Override
    public PulsarProducer create(Properties props) throws ResourceCreationException {

      String topic = props.getProperty(TOPIC_NAME);
      ProducerConfiguration config = (ProducerConfiguration) props.get(PRODUCER_CONFIG);

      try {
        // If we have a ProducerConfiguration then use it, otherwise a topic name will suffice
        return (config == null) ? new PulsarProducer(client.createProducer(topic), topic) :
              new PulsarProducer(client.createProducer(topic, config), topic);
       } catch (Exception e) {
         throw new ResourceCreationException(e);
       }
    }

}
