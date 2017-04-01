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
package org.apache.nifi.processors.standard;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.JMSException;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.util.JmsFactory;
import org.apache.nifi.processors.standard.util.WrappedMessageConsumer;

@Deprecated
@DeprecationNotice(classNames = {"org.apache.nifi.jms.processors.ConsumeJMS"}, reason = "This processor is deprecated and may be removed in future releases. ")
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"jms", "queue", "listen", "get", "pull", "source", "consume", "consumer"})
@CapabilityDescription("Pulls messages from a ActiveMQ JMS Queue, creating a FlowFile for each JMS Message or bundle of messages, as configured")
@SeeAlso({PutJMS.class})
public class GetJMSQueue extends JmsConsumer {

    private final Queue<WrappedMessageConsumer> consumerQueue = new LinkedBlockingQueue<>();

    @OnStopped
    public void cleanupResources() {
        WrappedMessageConsumer wrappedConsumer = consumerQueue.poll();
        while (wrappedConsumer != null) {
            wrappedConsumer.close(getLogger());
            wrappedConsumer = consumerQueue.poll();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        WrappedMessageConsumer wrappedConsumer = consumerQueue.poll();
        if (wrappedConsumer == null) {
            try {
                wrappedConsumer = JmsFactory.createQueueMessageConsumer(context);
            } catch (JMSException e) {
                logger.error("Failed to connect to JMS Server due to {}", e);
                context.yield();
                return;
            }
        }

        try {
            super.consume(context, session, wrappedConsumer);
        } finally {
            if (!wrappedConsumer.isClosed()) {
                consumerQueue.offer(wrappedConsumer);
            }
        }
    }

}
