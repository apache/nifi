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

import com.box.sdk.BoxAPIException;
import com.box.sdk.BoxAPIResponseException;
import com.box.sdk.BoxCollaboration;
import com.box.sdk.BoxCollaborator;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxGroup;
import com.box.sdk.BoxResourceIterable;
import com.box.sdk.BoxUser;
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

import static com.box.sdk.BoxCollaboration.Role.CO_OWNER;
import static com.box.sdk.BoxCollaboration.Role.EDITOR;
import static com.box.sdk.BoxCollaboration.Role.PREVIEWER;
import static com.box.sdk.BoxCollaboration.Role.VIEWER;
import static com.box.sdk.BoxCollaboration.Role.VIEWER_UPLOADER;
import static com.box.sdk.BoxCollaboration.Status.ACCEPTED;
import static com.box.sdk.BoxCollaboration.Status.PENDING;
import static com.box.sdk.BoxCollaborator.CollaboratorType.GROUP;
import static com.box.sdk.BoxCollaborator.CollaboratorType.USER;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GetBoxFileCollaboratorsTest extends AbstractBoxFileTest {
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
    BoxFile mockBoxFile;

    @Mock
    BoxCollaboration.Info mockCollabInfo1;

    @Mock
    BoxCollaboration.Info mockCollabInfo2;

    @Mock
    BoxCollaboration.Info mockCollabInfo3;

    @Mock
    BoxCollaboration.Info mockCollabInfo4;

    @Mock
    BoxCollaboration.Info mockCollabInfo5;

    @Mock
    BoxUser.Info mockUserInfo1;

    @Mock
    BoxUser.Info mockUserInfo2;

    @Mock
    BoxUser.Info mockUserInfo3;

    @Mock
    BoxGroup.Info mockGroupInfo1;

    @Mock
    BoxGroup.Info mockGroupInfo2;

    @Mock
    BoxResourceIterable<BoxCollaboration.Info> mockCollabIterable;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        final GetBoxFileCollaborators testSubject = new GetBoxFileCollaborators() {
            @Override
            protected BoxFile getBoxFile(String fileId) {
                return mockBoxFile;
            }
        };

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

        final BoxAPIResponseException mockException = new BoxAPIResponseException("API Error", 404, "Box File Not Found", null);
        when(mockBoxFile.getAllFileCollaborations()).thenThrow(mockException);

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetBoxFileCollaborators.REL_NOT_FOUND, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetBoxFileCollaborators.REL_NOT_FOUND);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();
        flowFilesFirst.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "404");
        flowFilesFirst.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "API Error [404]");
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

        setupCollaborator(mockCollabInfo1, mockUserInfo1, USER, TEST_USER_ID_1, ACCEPTED, CO_OWNER);
        setupCollaborator(mockCollabInfo2, mockUserInfo2, USER, TEST_USER_ID_2, ACCEPTED, EDITOR);
        setupCollaborator(mockCollabInfo3, mockUserInfo3, USER, TEST_USER_ID_3, ACCEPTED, VIEWER);
        setupCollaborator(mockCollabInfo4, mockGroupInfo1, GROUP, TEST_GROUP_ID_1, ACCEPTED, VIEWER_UPLOADER);
        setupCollaborator(mockCollabInfo5, mockGroupInfo2, GROUP, TEST_GROUP_ID_2, ACCEPTED, PREVIEWER);
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

        final BoxAPIException mockException = new BoxAPIException("General API Error:", 500, "Unexpected Error");
        when(mockBoxFile.getAllFileCollaborations()).thenThrow(mockException);

        final MockFlowFile inputFlowFile = new MockFlowFile(0);
        testRunner.enqueue(inputFlowFile);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(GetBoxFileCollaborators.REL_FAILURE, 1);
        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(GetBoxFileCollaborators.REL_FAILURE);
        final MockFlowFile flowFilesFirst = flowFiles.getFirst();

        flowFilesFirst.assertAttributeEquals(BoxFileAttributes.ERROR_CODE, "500");
        flowFilesFirst.assertAttributeEquals(BoxFileAttributes.ERROR_MESSAGE, "General API Error:\nUnexpected Error");
    }

    private void setupMockCollaborations() {
        setupCollaborator(mockCollabInfo1, mockUserInfo1, USER, TEST_USER_ID_1, ACCEPTED);
        setupCollaborator(mockCollabInfo2, mockUserInfo2, USER, TEST_USER_ID_2, ACCEPTED);
        setupCollaborator(mockCollabInfo3, mockGroupInfo1, GROUP, TEST_GROUP_ID_1, ACCEPTED);
        setupCollaborator(mockCollabInfo4, mockUserInfo3, USER, TEST_USER_ID_3, PENDING);
        setupCollaborator(mockCollabInfo5, mockGroupInfo2, GROUP, TEST_GROUP_ID_2, PENDING);

        setupFileCollaborations();
    }

    private void setupMockCollaborationsWithMultipleRoles() {
        // Editor role collaborators
        lenient().when(mockCollabInfo1.getAccessibleBy()).thenReturn(mockUserInfo1);
        lenient().when(mockUserInfo1.getType()).thenReturn(USER);
        lenient().when(mockUserInfo1.getID()).thenReturn(TEST_USER_ID_1);
        lenient().when(mockCollabInfo1.getStatus()).thenReturn(ACCEPTED);
        lenient().when(mockCollabInfo1.getRole()).thenReturn(EDITOR);
        lenient().when(mockUserInfo1.getLogin()).thenReturn(TEST_USER_EMAIL_1);
        // Editor role collaborator
        lenient().when(mockCollabInfo2.getAccessibleBy()).thenReturn(mockUserInfo2);
        lenient().when(mockUserInfo2.getType()).thenReturn(USER);
        lenient().when(mockUserInfo2.getID()).thenReturn(TEST_USER_ID_2);
        lenient().when(mockCollabInfo2.getStatus()).thenReturn(ACCEPTED);
        lenient().when(mockCollabInfo2.getRole()).thenReturn(EDITOR);
        lenient().when(mockUserInfo2.getLogin()).thenReturn(TEST_USER_EMAIL_2);
        // Editor role collaborator
        lenient().when(mockCollabInfo3.getAccessibleBy()).thenReturn(mockGroupInfo1);
        lenient().when(mockGroupInfo1.getType()).thenReturn(GROUP);
        lenient().when(mockGroupInfo1.getID()).thenReturn(TEST_GROUP_ID_1);
        lenient().when(mockCollabInfo3.getStatus()).thenReturn(ACCEPTED);
        lenient().when(mockCollabInfo3.getRole()).thenReturn(EDITOR);
        lenient().when(mockGroupInfo1.getLogin()).thenReturn(TEST_GROUP_EMAIL_1);
        // Viewer role collaborator
        lenient().when(mockCollabInfo4.getAccessibleBy()).thenReturn(mockUserInfo3);
        lenient().when(mockUserInfo3.getType()).thenReturn(USER);
        lenient().when(mockUserInfo3.getID()).thenReturn(TEST_USER_ID_3);
        lenient().when(mockCollabInfo4.getStatus()).thenReturn(ACCEPTED);
        lenient().when(mockCollabInfo4.getRole()).thenReturn(VIEWER);
        lenient().when(mockUserInfo3.getLogin()).thenReturn(TEST_USER_EMAIL_3);
        // Pending collaborators - should be filtered out by status filter
        lenient().when(mockCollabInfo5.getAccessibleBy()).thenReturn(mockGroupInfo2);
        lenient().when(mockGroupInfo2.getType()).thenReturn(GROUP);
        lenient().when(mockGroupInfo2.getID()).thenReturn(TEST_GROUP_ID_2);
        lenient().when(mockCollabInfo5.getStatus()).thenReturn(PENDING);
        lenient().when(mockCollabInfo5.getRole()).thenReturn(EDITOR);
        lenient().when(mockGroupInfo2.getLogin()).thenReturn(TEST_GROUP_EMAIL_2);

        setupFileCollaborations();
    }

    private void setupCollaborator(final BoxCollaboration.Info collabInfo,
                                   final BoxCollaborator.Info collaboratorInfo,
                                   final BoxCollaborator.CollaboratorType type,
                                   final String id,
                                   final BoxCollaboration.Status status) {
        setupCollaborator(collabInfo, collaboratorInfo, type, id, status, EDITOR);
    }

    private void setupCollaborator(final BoxCollaboration.Info collabInfo,
                                   final BoxCollaborator.Info collaboratorInfo,
                                   final BoxCollaborator.CollaboratorType type,
                                   final String id,
                                   final BoxCollaboration.Status status,
                                   final BoxCollaboration.Role role) {
        lenient().when(collabInfo.getAccessibleBy()).thenReturn(collaboratorInfo);
        lenient().when(collaboratorInfo.getType()).thenReturn(type);
        lenient().when(collaboratorInfo.getID()).thenReturn(id);
        lenient().when(collabInfo.getStatus()).thenReturn(status);
        lenient().when(collabInfo.getRole()).thenReturn(role);

        final Map<String, String> userEmails = Map.of(
                TEST_USER_ID_1, TEST_USER_EMAIL_1,
                TEST_USER_ID_2, TEST_USER_EMAIL_2,
                TEST_USER_ID_3, TEST_USER_EMAIL_3
        );

        Map<String, String> groupEmails = Map.of(
                TEST_GROUP_ID_1, TEST_GROUP_EMAIL_1,
                TEST_GROUP_ID_2, TEST_GROUP_EMAIL_2
        );
        String email = null;
        if (type.equals(USER)) {
            email = userEmails.getOrDefault(id, null);
        } else if (type.equals(GROUP)) {
            email = groupEmails.getOrDefault(id, null);
        }

        lenient().when(collaboratorInfo.getLogin()).thenReturn(email);
    }

    private void setupFileCollaborations() {
        lenient().when(mockCollabIterable.iterator()).thenReturn(
                List.of(mockCollabInfo1, mockCollabInfo2, mockCollabInfo3, mockCollabInfo4, mockCollabInfo5).iterator()
        );

        lenient().when(mockBoxFile.getAllFileCollaborations()).thenReturn(mockCollabIterable);
    }
}
