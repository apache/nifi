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
package org.apache.nifi.registry.web.api;

import org.apache.nifi.registry.authorization.User;
import org.apache.nifi.registry.authorization.UserGroup;
import org.apache.nifi.registry.event.EventFactory;
import org.apache.nifi.registry.event.EventService;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.apache.nifi.registry.revision.web.ClientIdParameter;
import org.apache.nifi.registry.revision.web.LongParameter;
import org.apache.nifi.registry.web.service.ServiceFacade;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.net.URISyntaxException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TenantResourceTest {

    private TenantResource tenantResource;
    private EventService eventService;
    private ServiceFacade serviceFacade;

    @Before
    public void setUp() {
        eventService = mock(EventService.class);
        serviceFacade = mock(ServiceFacade.class);

        tenantResource = new TenantResource(serviceFacade, eventService) {

            @Override
            protected URI getBaseUri() {
                try {
                    return new URI("http://registry.nifi.apache.org/nifi-registry");
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Test
    public void testCreateUser() {
        HttpServletRequest request = mock(HttpServletRequest.class);

        RevisionInfo revisionInfo = new RevisionInfo("client1", 0L);
        User user = new User(null, "identity");
        user.setRevision(revisionInfo);

        RevisionInfo resultRevisionInfo = new RevisionInfo("client1", 1L);
        User result = new User("identifier", user.getIdentity());
        result.setRevision(resultRevisionInfo);

        when(serviceFacade.createUser(user)).thenReturn(result);

        tenantResource.createUser(request, user);

        verify(serviceFacade).createUser(user);
        verify(eventService).publish(eq(EventFactory.userCreated(result)));
    }

    @Test
    public void testUpdateUser() {
        HttpServletRequest request = mock(HttpServletRequest.class);

        RevisionInfo revisionInfo = new RevisionInfo("client1", 1L);
        User user = new User("identifier", "new-identity");
        user.setRevision(revisionInfo);

        when(serviceFacade.updateUser(user)).thenReturn(user);

        tenantResource.updateUser(request, user.getIdentifier(), user);

        verify(serviceFacade).updateUser(user);
        verify(eventService).publish(eq(EventFactory.userUpdated(user)));
    }

    @Test
    public void testDeleteUser() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        User user = new User("identifier", "identity");
        Long version = new Long(0);
        ClientIdParameter clientId = new ClientIdParameter();

        when(serviceFacade.deleteUser(eq(user.getIdentifier()), any(RevisionInfo.class))).thenReturn(user);

        tenantResource.removeUser(request, new LongParameter(version.toString()), clientId, user.getIdentifier());

        verify(serviceFacade).deleteUser(eq(user.getIdentifier()), any(RevisionInfo.class));
        verify(eventService).publish(eq(EventFactory.userDeleted(user)));
    }

    @Test
    public void testCreateUserGroup() {
        HttpServletRequest request = mock(HttpServletRequest.class);

        RevisionInfo revisionInfo = new RevisionInfo("client1", 0L);
        UserGroup userGroup = new UserGroup(null, "identity");
        userGroup.setRevision(revisionInfo);

        RevisionInfo resultRevisionInfo = new RevisionInfo("client1", 1L);
        UserGroup result = new UserGroup("identifier", userGroup.getIdentity());
        result.setRevision(resultRevisionInfo);

        when(serviceFacade.createUserGroup(userGroup)).thenReturn(result);

        tenantResource.createUserGroup(request, userGroup);

        verify(serviceFacade).createUserGroup(userGroup);
        verify(eventService).publish(eq(EventFactory.userGroupCreated(result)));
    }

    @Test
    public void testUpdateUserGroup() {
        HttpServletRequest request = mock(HttpServletRequest.class);

        RevisionInfo revisionInfo = new RevisionInfo("client1", 1L);
        UserGroup userGroup = new UserGroup("identifier", "new-identity");
        userGroup.setRevision(revisionInfo);

        when(serviceFacade.updateUserGroup(userGroup)).thenReturn(userGroup);

        tenantResource.updateUserGroup(request, userGroup.getIdentifier(), userGroup);

        verify(serviceFacade).updateUserGroup(userGroup);
        verify(eventService).publish(eq(EventFactory.userGroupUpdated(userGroup)));
    }

    @Test
    public void testDeleteUserGroup() {
        HttpServletRequest request = mock(HttpServletRequest.class);
        UserGroup userGroup = new UserGroup("identifier", "identity");
        Long version = new Long(0);
        ClientIdParameter clientId = new ClientIdParameter();

        when(serviceFacade.deleteUserGroup(eq(userGroup.getIdentifier()), any(RevisionInfo.class))).thenReturn(userGroup);

        tenantResource.removeUserGroup(request, new LongParameter(version.toString()), clientId, userGroup.getIdentifier());

        verify(serviceFacade).deleteUserGroup(eq(userGroup.getIdentifier()), any(RevisionInfo.class));
        verify(eventService).publish(eq(EventFactory.userGroupDeleted(userGroup)));
    }
}
