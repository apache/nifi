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
package org.apache.nifi.amqp.processors;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.junit.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;

public class AMQPUtilsTest {


    @Test
    public void validateUpdateFlowFileAttributesWithAmqpProperties() {
        PublishAMQP processor = new PublishAMQP();
        ProcessSession processSession = new MockProcessSession(new SharedSessionState(processor, new AtomicLong()),
                processor);
        FlowFile sourceFlowFile = processSession.create();
        BasicProperties amqpProperties = new AMQP.BasicProperties.Builder()
                .contentType("text/plain").deliveryMode(2)
                .priority(1).userId("joe")
                .build();
        FlowFile f2 = AMQPUtils.updateFlowFileAttributesWithAmqpProperties(amqpProperties, sourceFlowFile,
                processSession);

        assertEquals("text/plain", f2.getAttributes().get(AMQPUtils.AMQP_PROP_PREFIX + "contentType"));
        assertEquals("joe", f2.getAttributes().get(AMQPUtils.AMQP_PROP_PREFIX + "userId"));
        assertEquals("2", f2.getAttributes().get(AMQPUtils.AMQP_PROP_PREFIX + "deliveryMode"));
    }
}
