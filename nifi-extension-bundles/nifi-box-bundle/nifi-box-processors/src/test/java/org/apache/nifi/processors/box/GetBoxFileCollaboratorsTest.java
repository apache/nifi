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
import com.box.sdkgen.managers.listcollaborations.ListCollaborationsManager;
import com.box.sdkgen.schemas.collaboration.Collaboration;
import com.box.sdkgen.schemas.collaboration.CollaborationRoleField;
import com.box.sdkgen.schemas.collaboration.CollaborationStatusField;
import com.box.sdkgen.schemas.collaborationaccessgrantee.CollaborationAccessGrantee;
import com.box.sdkgen.schemas.collaborations.Collaborations;
import com.box.sdkgen.schemas.groupmini.GroupMini;
import com.box.sdkgen.schemas.usercollaborations.UserCollaborations;
import com.box.sdkgen.serialization.json.EnumWrapper;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GetBoxFileCollaboratorsTest extends AbstractBoxFileTest implements FileListingTestTrait {
    private static final String TEST_USER_ID_1 = "user1";
    private static final String TEST_USER_ID_2 = "user2";
    private static final String TEST_USER_ID_3 = "user3";
    private static final String TEST_GROUP_ID_1 = "group1";
    private static final String TEST_GROUP_ID_2 = "group2";
    private static final String TEST_USER_EMAIL_1 = "user1@example.com";
    private static final String TEST_USER_EMAIL_2 = "user2@example.com";
    private static final String TEST_USER_EMAIL_3 = "user3@example.com";
    private static final String TEST_GROUP_EMAIL_1 = "group1@example.com";
    private static final String TEST_GROUP_EMAIL_2 = "group2@example.com";

    @Mock
    ListCollaborationsManager mockListCollaborationsManager;

    @Mock
    Collaboration mockCollabInfo1;

    @Mock
    Collaboration mockCollabInfo2;

    @Mock
    Collaboration mockCollabInfo3;

    @Mock
    Collaboration mockCollabInfo4;

    @Mock
    Collaboration mockCollabInfo5;

    @Mock
    UserCollaborations mockUserInfo1;

    @Mock
    UserCollaborations mockUserInfo2;

    @Mock
    UserCollaborations mockUserInfo3;

    @Mock
    GroupMini mockGroupInfo1;

    @Mock
    GroupMini mockGroupInfo2;

    @Mock
    Collaborations mockCollaborations;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final GetBoxFileCollaborators testSubject = new GetBoxFileCollaborators();

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    @Test
    void testGetCollaborationsFromFlowFileAttribute() {
        testRunner.setProperty(GetBoxFileCollaborators.FILE_ID, "${box.id}");
        testRunner.setProperty(GetBoxFileCollaborators.ROLES, "editor");
        testRunner.setProperty(GetBoxFileCollaborators.STATUSES, "accepted,pending");

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(BoxFileAttributes.ID, TEST_FILE_ID);
        inputFlowFile.putAttributes(attributes);

        setupMockCollaborations();

        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetBoxFileCollaborators.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetBoxFileCollaborators.REL_SUCCESS);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();

        flowFilesFirst.assertAttributeEquals("box.collaborations.count", "5");
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.users.ids", TEST_USER_ID_1 + "," + TEST_USER_ID_2);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.groups.ids", TEST_GROUP_ID_1);
        flowFilesFirst.assertAttributeEquals("box.collaborations.pending.editor.users.ids", TEST_USER_ID_3);
        flowFilesFirst.assertAttributeEquals("box.collaborations.pending.editor.groups.ids", TEST_GROUP_ID_2);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.users.logins", TEST_USER_EMAIL_1 + "," + TEST_USER_EMAIL_2);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.groups.emails", TEST_GROUP_EMAIL_1);
        flowFilesFirst.assertAttributeEquals("box.collaborations.pending.editor.users.logins", TEST_USER_EMAIL_3);
        flowFilesFirst.assertAttributeEquals("box.collaborations.pending.editor.groups.emails", TEST_GROUP_EMAIL_2);
    }

    @Test
    void testGetCollaborationsFromProperty() {
        testRunner.setProperty(GetBoxFileCollaborators.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(GetBoxFileCollaborators.ROLES, "editor");
        testRunner.setProperty(GetBoxFileCollaborators.STATUSES, "accepted");

        setupMockCollaborations();

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetBoxFileCollaborators.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetBoxFileCollaborators.REL_SUCCESS);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();

        flowFilesFirst.assertAttributeEquals("box.collaborations.count", "5");
        // New format attributes
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.users.ids", TEST_USER_ID_1 + "," + TEST_USER_ID_2);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.groups.ids", TEST_GROUP_ID_1);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.users.logins", TEST_USER_EMAIL_1 + "," + TEST_USER_EMAIL_2);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.groups.emails", TEST_GROUP_EMAIL_1);
        // For pending status items, they shouldn't appear since we're only checking accepted status
        flowFilesFirst.assertAttributeNotExists("box.collaborations.pending.editor.users.ids");
        flowFilesFirst.assertAttributeNotExists("box.collaborations.pending.editor.groups.ids");
        flowFilesFirst.assertAttributeNotExists("box.collaborations.pending.editor.users.logins");
        flowFilesFirst.assertAttributeNotExists("box.collaborations.pending.editor.groups.emails");
    }

    @Test
    void testGetCollaborationsBackwardCompatibility() {
        testRunner.setProperty(GetBoxFileCollaborators.FILE_ID, TEST_FILE_ID);
        // Don't set ROLES or STATUSES properties to test backward compatibility mode

        setupMockCollaborations();

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetBoxFileCollaborators.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetBoxFileCollaborators.REL_SUCCESS);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();

        flowFilesFirst.assertAttributeEquals("box.collaborations.count", "5");
        // Backward compatibility attributes should exist
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.users.ids", TEST_USER_ID_1 + "," + TEST_USER_ID_2);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.groups.ids", TEST_GROUP_ID_1);
        flowFilesFirst.assertAttributeEquals("box.collaborations.pending.users.ids", TEST_USER_ID_3);
        flowFilesFirst.assertAttributeEquals("box.collaborations.pending.groups.ids", TEST_GROUP_ID_2);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.users.emails", TEST_USER_EMAIL_1 + "," + TEST_USER_EMAIL_2);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.groups.emails", TEST_GROUP_EMAIL_1);
        flowFilesFirst.assertAttributeEquals("box.collaborations.pending.users.emails", TEST_USER_EMAIL_3);
        flowFilesFirst.assertAttributeEquals("box.collaborations.pending.groups.emails", TEST_GROUP_EMAIL_2);

        // New format attributes should not exist
        flowFilesFirst.assertAttributeNotExists("box.collaborations.accepted.editor.users.ids");
        flowFilesFirst.assertAttributeNotExists("box.collaborations.accepted.editor.groups.ids");
        flowFilesFirst.assertAttributeNotExists("box.collaborations.accepted.editor.users.logins");
        flowFilesFirst.assertAttributeNotExists("box.collaborations.accepted.editor.groups.emails");
    }

    @Test
    void testApiErrorHandling() {
        testRunner.setProperty(GetBoxFileCollaborators.FILE_ID, TEST_FILE_ID);

        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(404);
        BoxAPIError mockException = mock(BoxAPIError.class);
        when(mockException.getMessage()).thenReturn("API Error [404]");
        when(mockException.getResponseInfo()).thenReturn(mockResponseInfo);

        when(mockListCollaborationsManager.getFileCollaborations(anyString())).thenThrow(mockException);
        when(mockBoxClient.getListCollaborations()).thenReturn(mockListCollaborationsManager);

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetBoxFileCollaborators.REL_NOT_FOUND, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetBoxFileCollaborators.REL_NOT_FOUND);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();
        flowFilesFirst.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
    }

    @Test
    void testGetCollaborationsWithMultipleRoles() {
        testRunner.setProperty(GetBoxFileCollaborators.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(GetBoxFileCollaborators.ROLES, "editor,viewer");
        testRunner.setProperty(GetBoxFileCollaborators.STATUSES, "accepted");

        setupMockCollaborationsWithMultipleRoles();

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetBoxFileCollaborators.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetBoxFileCollaborators.REL_SUCCESS);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();

        flowFilesFirst.assertAttributeEquals("box.collaborations.count", "5");
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.users.ids", TEST_USER_ID_1 + "," + TEST_USER_ID_2);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.groups.ids", TEST_GROUP_ID_1);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.viewer.users.ids", TEST_USER_ID_3);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.users.logins", TEST_USER_EMAIL_1 + "," + TEST_USER_EMAIL_2);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.groups.emails", TEST_GROUP_EMAIL_1);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.viewer.users.logins", TEST_USER_EMAIL_3);
    }

    @Test
    void testGetCollaborationsForAllViewingRoles() {
        testRunner.setProperty(GetBoxFileCollaborators.FILE_ID, TEST_FILE_ID);
        testRunner.setProperty(GetBoxFileCollaborators.ROLES, "owner,co-owner,editor,viewer uploader,viewer");
        testRunner.setProperty(GetBoxFileCollaborators.STATUSES, "accepted");

        setupCollaborator(mockCollabInfo1, mockUserInfo1, true, TEST_USER_ID_1, CollaborationStatusField.ACCEPTED, CollaborationRoleField.CO_OWNER);
        setupCollaborator(mockCollabInfo2, mockUserInfo2, true, TEST_USER_ID_2, CollaborationStatusField.ACCEPTED, CollaborationRoleField.EDITOR);
        setupCollaborator(mockCollabInfo3, mockUserInfo3, true, TEST_USER_ID_3, CollaborationStatusField.ACCEPTED, CollaborationRoleField.VIEWER);
        setupCollaborator(mockCollabInfo4, mockGroupInfo1, false, TEST_GROUP_ID_1, CollaborationStatusField.ACCEPTED, CollaborationRoleField.VIEWER_UPLOADER);
        setupCollaborator(mockCollabInfo5, mockGroupInfo2, false, TEST_GROUP_ID_2, CollaborationStatusField.ACCEPTED, CollaborationRoleField.PREVIEWER);
        setupFileCollaborations();

        testRunner.enqueue(new MockFlowFile(0));
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetBoxFileCollaborators.REL_SUCCESS, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetBoxFileCollaborators.REL_SUCCESS);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();

        flowFilesFirst.assertAttributeEquals("box.collaborations.count", "5");
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.co-owner.users.ids", TEST_USER_ID_1);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.co-owner.users.logins", TEST_USER_EMAIL_1);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.users.ids", TEST_USER_ID_2);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.editor.users.logins", TEST_USER_EMAIL_2);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.viewer.users.ids", TEST_USER_ID_3);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.viewer.users.logins", TEST_USER_EMAIL_3);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.viewer uploader.groups.ids", TEST_GROUP_ID_1);
        flowFilesFirst.assertAttributeEquals("box.collaborations.accepted.viewer uploader.groups.emails", TEST_GROUP_EMAIL_1);
    }

    @Test
    void testBoxApiExceptionHandling() {
        testRunner.setProperty(GetBoxFileCollaborators.FILE_ID, TEST_FILE_ID);

        ResponseInfo mockResponseInfo = mock(ResponseInfo.class);
        when(mockResponseInfo.getStatusCode()).thenReturn(500);
        BoxAPIError mockException = mock(BoxAPIError.class);
        when(mockException.getMessage()).thenReturn("General API Error:\nUnexpected Error");
        when(mockException.getResponseInfo()).thenReturn(mockResponseInfo);

        when(mockListCollaborationsManager.getFileCollaborations(anyString())).thenThrow(mockException);
        when(mockBoxClient.getListCollaborations()).thenReturn(mockListCollaborationsManager);

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetBoxFileCollaborators.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetBoxFileCollaborators.REL_FAILURE);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();

        flowFilesFirst.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "500");
    }

    private void setupMockCollaborations() {
        setupCollaborator(mockCollabInfo1, mockUserInfo1, true, TEST_USER_ID_1, CollaborationStatusField.ACCEPTED, CollaborationRoleField.EDITOR);
        setupCollaborator(mockCollabInfo2, mockUserInfo2, true, TEST_USER_ID_2, CollaborationStatusField.ACCEPTED, CollaborationRoleField.EDITOR);
        setupCollaborator(mockCollabInfo3, mockGroupInfo1, false, TEST_GROUP_ID_1, CollaborationStatusField.ACCEPTED, CollaborationRoleField.EDITOR);
        setupCollaborator(mockCollabInfo4, mockUserInfo3, true, TEST_USER_ID_3, CollaborationStatusField.PENDING, CollaborationRoleField.EDITOR);
        setupCollaborator(mockCollabInfo5, mockGroupInfo2, false, TEST_GROUP_ID_2, CollaborationStatusField.PENDING, CollaborationRoleField.EDITOR);

        setupFileCollaborations();
    }

    private void setupMockCollaborationsWithMultipleRoles() {
        // Editor role collaborators
        setupCollaborator(mockCollabInfo1, mockUserInfo1, true, TEST_USER_ID_1, CollaborationStatusField.ACCEPTED, CollaborationRoleField.EDITOR);
        setupCollaborator(mockCollabInfo2, mockUserInfo2, true, TEST_USER_ID_2, CollaborationStatusField.ACCEPTED, CollaborationRoleField.EDITOR);
        setupCollaborator(mockCollabInfo3, mockGroupInfo1, false, TEST_GROUP_ID_1, CollaborationStatusField.ACCEPTED, CollaborationRoleField.EDITOR);
        // Viewer role collaborator
        setupCollaborator(mockCollabInfo4, mockUserInfo3, true, TEST_USER_ID_3, CollaborationStatusField.ACCEPTED, CollaborationRoleField.VIEWER);
        // Pending collaborators - should be filtered out by status filter
        setupCollaborator(mockCollabInfo5, mockGroupInfo2, false, TEST_GROUP_ID_2, CollaborationStatusField.PENDING, CollaborationRoleField.EDITOR);

        setupFileCollaborations();
    }

    private void setupCollaborator(final Collaboration collabInfo,
                                   final Object collaboratorInfo,
                                   final boolean isUser,
                                   final String id,
                                   final CollaborationStatusField status,
                                   final CollaborationRoleField role) {
        // Create a mock CollaborationAccessGrantee that wraps the collaborator
        CollaborationAccessGrantee mockAccessGrantee = mock(CollaborationAccessGrantee.class);
        lenient().when(mockAccessGrantee.isUserCollaborations()).thenReturn(isUser);
        lenient().when(mockAccessGrantee.isGroupMini()).thenReturn(!isUser);

        lenient().doReturn(mockAccessGrantee).when(collabInfo).getAccessibleBy();
        lenient().doReturn(new EnumWrapper<>(status)).when(collabInfo).getStatus();
        lenient().doReturn(new EnumWrapper<>(role)).when(collabInfo).getRole();

        final Map<String, String> userEmails = Map.of(
                TEST_USER_ID_1, TEST_USER_EMAIL_1,
                TEST_USER_ID_2, TEST_USER_EMAIL_2,
                TEST_USER_ID_3, TEST_USER_EMAIL_3
        );

        Map<String, String> groupEmails = Map.of(
                TEST_GROUP_ID_1, TEST_GROUP_EMAIL_1,
                TEST_GROUP_ID_2, TEST_GROUP_EMAIL_2
        );

        if (isUser && collaboratorInfo instanceof UserCollaborations user) {
            lenient().when(user.getId()).thenReturn(id);
            String email = userEmails.get(id);
            lenient().when(user.getLogin()).thenReturn(email);
            lenient().when(mockAccessGrantee.getUserCollaborations()).thenReturn(user);
        } else if (collaboratorInfo instanceof GroupMini group) {
            lenient().when(group.getId()).thenReturn(id);
            String email = groupEmails.get(id);
            lenient().when(group.getName()).thenReturn(email);
            lenient().when(mockAccessGrantee.getGroupMini()).thenReturn(group);
        }
    }

    private void setupFileCollaborations() {
        lenient().when(mockCollaborations.getEntries()).thenReturn(
                List.of(mockCollabInfo1, mockCollabInfo2, mockCollabInfo3, mockCollabInfo4, mockCollabInfo5)
        );

        lenient().when(mockListCollaborationsManager.getFileCollaborations(anyString())).thenReturn(mockCollaborations);
        lenient().when(mockBoxClient.getListCollaborations()).thenReturn(mockListCollaborationsManager);
    }

    @Override
    public BoxClient getMockBoxClient() {
        return mockBoxClient;
    }
}
