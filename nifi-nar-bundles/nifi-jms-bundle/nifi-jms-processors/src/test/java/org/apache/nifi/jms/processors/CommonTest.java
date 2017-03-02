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
package org.apache.nifi.jms.processors;

import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.ServiceLoader;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.nifi.processor.Processor;
import org.junit.Test;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;

public class CommonTest {

    @Test
    public void validateServiceIsLocatableViaServiceLoader() {
        ServiceLoader<Processor> loader = ServiceLoader.<Processor> load(Processor.class);
        Iterator<Processor> iter = loader.iterator();
        boolean pubJmsPresent = false;
        boolean consumeJmsPresent = false;
        while (iter.hasNext()) {
            Processor p = iter.next();
            if (p.getClass().getSimpleName().equals(PublishJMS.class.getSimpleName())) {
                pubJmsPresent = true;
            } else if (p.getClass().getSimpleName().equals(ConsumeJMS.class.getSimpleName())) {
                consumeJmsPresent = true;
            }

        }
        assertTrue(pubJmsPresent);
        assertTrue(consumeJmsPresent);
    }

    static JmsTemplate buildJmsTemplateForDestination(boolean pubSub) {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "vm://localhost?broker.persistent=false");
        connectionFactory = new CachingConnectionFactory(connectionFactory);

        JmsTemplate jmsTemplate = new JmsTemplate(connectionFactory);
        jmsTemplate.setPubSubDomain(pubSub);
        jmsTemplate.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        return jmsTemplate;
    }
}
