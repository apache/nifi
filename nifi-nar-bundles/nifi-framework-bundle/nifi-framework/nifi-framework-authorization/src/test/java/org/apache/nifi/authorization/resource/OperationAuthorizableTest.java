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
package org.apache.nifi.authorization.resource;

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.MockPolicyBasedAuthorizer;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.junit.Test;

import static org.apache.nifi.authorization.RequestAction.READ;
import static org.apache.nifi.authorization.RequestAction.WRITE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class OperationAuthorizableTest {

    private static final User AUTH_USER = new User.Builder()
            .identity("user-a")
            .identifierGenerateRandom()
            .build();
    private static final StandardNiFiUser USER = new StandardNiFiUser.Builder()
            .identity(AUTH_USER.getIdentity())
            .build();
    private final MockProcessGroup ROOT_PG = new MockProcessGroup("root", null);
    private final MockProcessGroup PG1 = new MockProcessGroup("pg-1", ROOT_PG);
    private final Authorizable PROCESSOR = new MockProcessor("component-1", PG1);

    private class MockProcessGroup implements Authorizable {
        private final String identifier;
        private final MockProcessGroup parent;

        private MockProcessGroup(String identifier, MockProcessGroup parent) {
            this.identifier = identifier;
            this.parent = parent;
        }

        public String getIdentifier() {
            return identifier;
        }

        @Override
        public Authorizable getParentAuthorizable() {
            return parent;
        }

        @Override
        public Resource getResource() {
            return ResourceFactory.getComponentResource(ResourceType.ProcessGroup, identifier, identifier);
        }
    }

    private class MockProcessor implements ComponentAuthorizable {
        private final String identifier;
        private final MockProcessGroup processGroup;

        private MockProcessor(String identifier, MockProcessGroup processGroup) {
            this.identifier = identifier;
            this.processGroup = processGroup;
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public String getProcessGroupIdentifier() {
            return processGroup.getIdentifier();
        }

        @Override
        public Authorizable getParentAuthorizable() {
            return processGroup;
        }

        @Override
        public Resource getResource() {
            return ResourceFactory.getComponentResource(ResourceType.Processor, identifier, identifier);
        }
    }

    private void shouldBeDenied(String message, Runnable test) {
        try {
            test.run();
            fail(message);
        } catch (AccessDeniedException e) {
            assertNotNull(e);
        }
    }

    @Test()
    public void testUnauthorizedRead() {
        final MockPolicyBasedAuthorizer authorizer = new MockPolicyBasedAuthorizer();

        // The user should not be able to access the component in any way.
        shouldBeDenied("Component WRITE should be denied",
                () -> PROCESSOR.authorize(authorizer, WRITE, USER));

        shouldBeDenied("Component READ should be denied",
                () -> PROCESSOR.authorize(authorizer, READ, USER));

        shouldBeDenied("Operation should be denied",
                () -> OperationAuthorizable.authorizeOperation(PROCESSOR, authorizer, USER));
    }

    @Test()
    public void testAuthorizedByComponentRead() {
        final MockPolicyBasedAuthorizer authorizer = new MockPolicyBasedAuthorizer();
        authorizer.addUser(AUTH_USER);
        authorizer.addAccessPolicy(new AccessPolicy.Builder()
                .identifierGenerateRandom()
                .addUser(AUTH_USER.getIdentifier())
                .resource("/processors/component-1")
                .action(READ)
                .build());

        PROCESSOR.authorize(authorizer, READ, USER);

        // If the user has only READ access to the base component WRITE and operation should be denied
        shouldBeDenied("Component WRITE should be denied",
                () -> PROCESSOR.authorize(authorizer, WRITE, USER));

        shouldBeDenied("Operation WRITE should be denied",
                () -> OperationAuthorizable.authorizeOperation(PROCESSOR, authorizer, USER));
    }

    @Test()
    public void testAuthorizedByComponentWrite() {
        final MockPolicyBasedAuthorizer authorizer = new MockPolicyBasedAuthorizer();
        authorizer.addUser(AUTH_USER);
        authorizer.addAccessPolicy(new AccessPolicy.Builder()
                .identifierGenerateRandom()
                .addUser(AUTH_USER.getIdentifier())
                .resource("/processors/component-1")
                .action(WRITE)
                .build());

        // If the user has WRITE access to the base component, operation access should be allowed, too
        PROCESSOR.authorize(authorizer, WRITE, USER);
        OperationAuthorizable.authorizeOperation(PROCESSOR, authorizer, USER);

        // But READ should be denied
        shouldBeDenied("Component READ should be denied",
                () -> PROCESSOR.authorize(authorizer, READ, USER));
    }

    @Test()
    public void testAuthorizedByComponentParentWrite() {
        final MockPolicyBasedAuthorizer authorizer = new MockPolicyBasedAuthorizer();
        authorizer.addUser(AUTH_USER);
        authorizer.addAccessPolicy(new AccessPolicy.Builder()
                .identifierGenerateRandom()
                .addUser(AUTH_USER.getIdentifier())
                .resource("/process-groups/root")
                .action(WRITE)
                .build());

        // If the user has WRITE access to the base component, operation access should be allowed, too
        PROCESSOR.authorize(authorizer, WRITE, USER);
        OperationAuthorizable.authorizeOperation(PROCESSOR, authorizer, USER);

        // But READ should be denied
        shouldBeDenied("Component READ should be denied",
                () -> PROCESSOR.authorize(authorizer, READ, USER));

    }

    @Test()
    public void testAuthorizedByOperationWrite() {
        final MockPolicyBasedAuthorizer authorizer = new MockPolicyBasedAuthorizer();
        authorizer.addUser(AUTH_USER);
        authorizer.addAccessPolicy(new AccessPolicy.Builder()
                .identifierGenerateRandom()
                .addUser(AUTH_USER.getIdentifier())
                .resource("/operation/processors/component-1")
                .action(WRITE)
                .build());

        // Operation should be allowed, too.
        OperationAuthorizable.authorizeOperation(PROCESSOR, authorizer, USER);

        // If the user only has the operation permissions, then component access should be denied.
        shouldBeDenied("Component READ should be denied",
                () -> PROCESSOR.authorize(authorizer, READ, USER));
        shouldBeDenied("Component WRITE should be denied",
                () -> PROCESSOR.authorize(authorizer, WRITE, USER));
    }

    @Test()
    public void testAuthorizedByOperationParentWrite() {
        final MockPolicyBasedAuthorizer authorizer = new MockPolicyBasedAuthorizer();
        authorizer.addUser(AUTH_USER);
        authorizer.addAccessPolicy(new AccessPolicy.Builder()
                .identifierGenerateRandom()
                .addUser(AUTH_USER.getIdentifier())
                .resource("/operation/process-groups/root")
                .action(WRITE)
                .build());

        // Operation should be allowed.
        OperationAuthorizable.authorizeOperation(PROCESSOR, authorizer, USER);

        // If the user only has the operation permissions, then component access should be denied.
        shouldBeDenied("Component READ should be denied",
                () -> PROCESSOR.authorize(authorizer, READ, USER));
        shouldBeDenied("Component WRITE should be denied",
                () -> PROCESSOR.authorize(authorizer, WRITE, USER));
    }
}
