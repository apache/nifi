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
package org.apache.nifi.remote;

import org.apache.nifi.authorization.AuthorizationRequest;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestStandardRootGroupPort {

    private RootGroupPort createRootGroupPort(NiFiProperties nifiProperties) {
        final BulletinRepository bulletinRepository = mock(BulletinRepository.class);
        final ProcessScheduler processScheduler = null;

        final Authorizer authorizer = mock(Authorizer.class);
        doAnswer(invocation -> {
            final AuthorizationRequest request = invocation.getArgumentAt(0, AuthorizationRequest.class);
            if ("node1@nifi.test".equals(request.getIdentity())) {
                return AuthorizationResult.approved();
            }
            return AuthorizationResult.denied();
        }).when(authorizer).authorize(any(AuthorizationRequest.class));

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        doReturn("process-group-id").when(processGroup).getIdentifier();

        return new StandardRootGroupPort("id", "name", processGroup,
                TransferDirection.SEND, ConnectableType.INPUT_PORT, authorizer, bulletinRepository,
                processScheduler, true, nifiProperties);
    }

    @Test
    public void testCheckUserAuthorizationByDn() {

        final NiFiProperties nifiProperties = mock(NiFiProperties.class);

        final RootGroupPort port = createRootGroupPort(nifiProperties);

        PortAuthorizationResult authResult = port.checkUserAuthorization("CN=node1, OU=nifi.test");
        Assert.assertFalse(authResult.isAuthorized());

        authResult = port.checkUserAuthorization("node1@nifi.test");
        Assert.assertTrue(authResult.isAuthorized());
    }

    @Test
    public void testCheckUserAuthorizationByMappedDn() {

        final NiFiProperties nifiProperties = mock(NiFiProperties.class);
        final String mapKey = ".dn";
        Set<String> propertyKeys = new LinkedHashSet<>();
        propertyKeys.add(NiFiProperties.SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX + mapKey);
        propertyKeys.add(NiFiProperties.SECURITY_IDENTITY_MAPPING_VALUE_PREFIX + mapKey);
        doReturn(propertyKeys).when(nifiProperties).getPropertyKeys();

        final String mapPattern = "^CN=(.*?), OU=(.*?)$";
        final String mapValue = "$1@$2";
        doReturn(mapPattern).when(nifiProperties).getProperty(eq(NiFiProperties.SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX + mapKey));
        doReturn(mapValue).when(nifiProperties).getProperty(eq(NiFiProperties.SECURITY_IDENTITY_MAPPING_VALUE_PREFIX + mapKey));

        final RootGroupPort port = createRootGroupPort(nifiProperties);

        PortAuthorizationResult authResult = port.checkUserAuthorization("CN=node2, OU=nifi.test");
        Assert.assertFalse(authResult.isAuthorized());

        authResult = port.checkUserAuthorization("CN=node1, OU=nifi.test");
        Assert.assertTrue(authResult.isAuthorized());
    }

}
