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
package org.apache.nifi.processors.box;

import com.box.sdkgen.box.errors.BoxAPIError;
import com.box.sdkgen.box.errors.ResponseInfo;
import com.box.sdkgen.client.BoxClient;
import com.box.sdkgen.schemas.groupmembership.GroupMembership;
import com.box.sdkgen.schemas.groupmemberships.GroupMemberships;
import com.box.sdkgen.schemas.usermini.UserMini;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static org.apache.nifi.processors.box.BoxGroupAttributes.GROUP_USER_IDS;
import static org.apache.nifi.processors.box.BoxGroupAttributes.GROUP_USER_LOGINS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GetBoxGroupMembersTest extends AbstractBoxFileTest implements FileListingTestTrait {

    private static final String VALID_GROUP_ID = "valid-group";
    private static final String NOT_FOUND_GROUP_ID = "not-found-group";
    private static final String ERROR_GROUP_ID = "error-group";

    private final AtomicReference<GroupMemberships> membershipsHolder = new AtomicReference<>();

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final GetBoxGroupMembers processor = new GetBoxGroupMembers() {
            @Override
            GroupMemberships retrieveGroupMemberships(final String groupId) {
                return switch (groupId) {
                    case VALID_GROUP_ID -> membershipsHolder.get();
                    case NOT_FOUND_GROUP_ID -> {
                        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
                        when(mockResponseInfo.getStatusCode()).thenReturn(404);
                        BoxAPIError exception = mock(BoxAPIError.class);
                        when(exception.getMessage()).thenReturn("Group not found");
                        when(exception.getResponseInfo()).thenReturn(mockResponseInfo);
                        throw exception;
                    }
                    case ERROR_GROUP_ID -> {
                        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
                        when(mockResponseInfo.getStatusCode()).thenReturn(400);
                        BoxAPIError exception = mock(BoxAPIError.class);
                        when(exception.getMessage()).thenReturn("Client error");
                        when(exception.getResponseInfo()).thenReturn(mockResponseInfo);
                        throw exception;
                    }
                    default -> throw new IllegalArgumentException("Unexpected group ID: " + groupId);
                };
            }
        };

        testRunner = TestRunners.newTestRunner(processor);
        super.setUp();
    }

    @AfterEach
    void tearDown() {
        membershipsHolder.set(null);
    }

    @Test
    void getMembers_forEmptyGroup() {
        GroupMemberships emptyMemberships = mock(GroupMemberships.class);
        when(emptyMemberships.getEntries()).thenReturn(emptyList());
        membershipsHolder.set(emptyMemberships);

        testRunner.enqueue(createFlowFile(VALID_GROUP_ID));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(GetBoxGroupMembers.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GetBoxGroupMembers.REL_SUCCESS).getFirst();
        assertTrue(flowFile.getAttribute(GROUP_USER_IDS).isEmpty());
        assertTrue(flowFile.getAttribute(GROUP_USER_LOGINS).isEmpty());
    }

    @Test
    void getMembers_forGroupWithSingleMember() {
        final GroupMembership member = createGroupMember("1", "1@mail.org");
        GroupMemberships memberships = mock(GroupMemberships.class);
        when(memberships.getEntries()).thenReturn(List.of(member));
        membershipsHolder.set(memberships);

        testRunner.enqueue(createFlowFile(VALID_GROUP_ID));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(GetBoxGroupMembers.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GetBoxGroupMembers.REL_SUCCESS).getFirst();
        assertEquals(flowFile.getAttribute(GROUP_USER_IDS), "1");
        assertEquals(flowFile.getAttribute(GROUP_USER_LOGINS), "1@mail.org");
    }

    @Test
    void getMembers_forGroupWithMultipleMembers() {
        final GroupMembership member1 = createGroupMember("1", "1@mail.org");
        final GroupMembership member2 = createGroupMember("2", "2@mail.org");
        final GroupMembership member3 = createGroupMember("3", "3@mail.org");
        final List<GroupMembership> members = List.of(member1, member2, member3);
        GroupMemberships memberships = mock(GroupMemberships.class);
        when(memberships.getEntries()).thenReturn(members);
        membershipsHolder.set(memberships);

        testRunner.enqueue(createFlowFile(VALID_GROUP_ID));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(GetBoxGroupMembers.REL_SUCCESS, 1);

        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GetBoxGroupMembers.REL_SUCCESS).getFirst();

        final String expectedUserIds = members.stream().map(m -> m.getUser().getId()).collect(joining(","));
        final String expectedUserLogins = members.stream().map(m -> m.getUser().getLogin()).collect(joining(","));

        assertEquals(flowFile.getAttribute(GROUP_USER_IDS), expectedUserIds);
        assertEquals(flowFile.getAttribute(GROUP_USER_LOGINS), expectedUserLogins);
    }

    @Test
    void getMembers_forNotFoundGroup_routesToNotFound() {
        testRunner.enqueue(createFlowFile(NOT_FOUND_GROUP_ID));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(GetBoxGroupMembers.REL_NOT_FOUND, 1);
    }

    @Test
    void getMembers_forBoxApiFailure_routesToFailure() {
        testRunner.enqueue(createFlowFile(ERROR_GROUP_ID));
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(GetBoxGroupMembers.REL_FAILURE, 1);
    }

    private GroupMembership createGroupMember(final String userId, final String userLogin) {
        UserMini user = mock(UserMini.class);
        when(user.getId()).thenReturn(userId);
        when(user.getLogin()).thenReturn(userLogin);

        GroupMembership membership = mock(GroupMembership.class);
        when(membership.getUser()).thenReturn(user);

        return membership;
    }

    private FlowFile createFlowFile(final String groupId) {
        final MockFlowFile flowFile = new MockFlowFile(0);
        flowFile.putAttributes(Map.of(BoxGroupAttributes.GROUP_ID, groupId));
        return flowFile;
    }

    @Override
    public BoxClient getMockBoxClient() {
        return mockBoxClient;
    }
}
