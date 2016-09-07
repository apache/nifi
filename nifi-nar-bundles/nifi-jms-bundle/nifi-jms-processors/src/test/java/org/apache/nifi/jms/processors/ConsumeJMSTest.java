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

import org.apache.nifi.jms.cf.JMSConnectionFactoryProviderDefinition;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsumeJMSTest {

    @Test
    public void validateSuccessfulConsumeAndTransferToSuccess() throws Exception {
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination("cooQueue", false);
        JMSPublisher sender = new JMSPublisher(jmsTemplate, mock(ComponentLog.class));
        sender.publish("Hey dude!".getBytes());
        TestRunner runner = TestRunners.newTestRunner(new ConsumeJMS());
        JMSConnectionFactoryProviderDefinition cs = mock(JMSConnectionFactoryProviderDefinition.class);
        when(cs.getIdentifier()).thenReturn("cfProvider");
        when(cs.getConnectionFactory()).thenReturn(jmsTemplate.getConnectionFactory());
        runner.addControllerService("cfProvider", cs);
        runner.enableControllerService(cs);

        runner.setProperty(PublishJMS.CF_SERVICE, "cfProvider");
        runner.setProperty(ConsumeJMS.DESTINATION, "cooQueue");
        runner.setProperty(ConsumeJMS.DESTINATION_TYPE, ConsumeJMS.QUEUE);
        runner.run(1, false);
        //
        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishJMS.REL_SUCCESS).get(0);
        assertNotNull(successFF);
        assertEquals("cooQueue", successFF.getAttributes().get(JmsHeaders.DESTINATION));
        successFF.assertContentEquals("Hey dude!".getBytes());
        String sourceDestination = successFF.getAttribute(ConsumeJMS.JMS_SOURCE_DESTINATION_NAME);
        assertNotNull(sourceDestination);

        ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
    }
}
