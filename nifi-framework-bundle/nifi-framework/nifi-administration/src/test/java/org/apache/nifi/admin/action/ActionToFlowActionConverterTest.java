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
package org.apache.nifi.admin.action;

import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowAction;
import org.apache.nifi.action.FlowActionAttribute;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.RequestDetails;
import org.apache.nifi.action.StandardRequestDetails;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class ActionToFlowActionConverterTest {
    private static final String SOURCE_ID = UUID.randomUUID().toString();

    private static final String REMOTE_ADDRESS = "127.0.0.1";

    private static final String FORWARDED_FOR = "127.0.0.127";

    private static final String USER_AGENT = "Apache NiFi";

    private static final String USER_IDENTITY = "username";

    private static final Component SOURCE_TYPE = Component.Label;

    private static final Operation OPERATION = Operation.Add;

    private final ActionToFlowActionConverter converter = new ActionToFlowActionConverter();

    @Test
    void testConvertRequestDetails() {
        final RequestDetails requestDetails = new StandardRequestDetails(REMOTE_ADDRESS, FORWARDED_FOR, USER_AGENT);

        final Date timestamp = new Date();

        final FlowChangeAction action = new FlowChangeAction();
        action.setUserIdentity(USER_IDENTITY);
        action.setSourceId(SOURCE_ID);
        action.setSourceType(SOURCE_TYPE);
        action.setOperation(OPERATION);
        action.setTimestamp(timestamp);
        action.setRequestDetails(requestDetails);

        final FlowAction flowAction = converter.convert(action);

        assertNotNull(flowAction);
        final Map<String, String> attributes = flowAction.getAttributes();
        assertNotNull(attributes);

        // Action ID is not expected to be populated
        assertNull(attributes.get(FlowActionAttribute.ACTION_ID.key()));

        final String timestampExpected = timestamp.toInstant().toString();
        assertEquals(timestampExpected, attributes.get(FlowActionAttribute.ACTION_TIMESTAMP.key()));
        assertEquals(SOURCE_ID, attributes.get(FlowActionAttribute.ACTION_SOURCE_ID.key()));
        assertEquals(SOURCE_TYPE.toString(), attributes.get(FlowActionAttribute.ACTION_SOURCE_TYPE.key()));
        assertEquals(OPERATION.toString(), attributes.get(FlowActionAttribute.ACTION_OPERATION.key()));

        assertEquals(REMOTE_ADDRESS, attributes.get(FlowActionAttribute.REQUEST_DETAILS_REMOTE_ADDRESS.key()));
        assertEquals(FORWARDED_FOR, attributes.get(FlowActionAttribute.REQUEST_DETAILS_FORWARDED_FOR.key()));
        assertEquals(USER_AGENT, attributes.get(FlowActionAttribute.REQUEST_DETAILS_USER_AGENT.key()));
    }
}
