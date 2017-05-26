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
package org.apache.nifi.web

import org.apache.nifi.authorization.AccessDeniedException
import org.apache.nifi.authorization.AccessPolicy
import org.apache.nifi.authorization.AuthorizableLookup
import org.apache.nifi.authorization.AuthorizationResult
import org.apache.nifi.authorization.Authorizer
import org.apache.nifi.authorization.Group
import org.apache.nifi.authorization.RequestAction
import org.apache.nifi.authorization.Resource
import org.apache.nifi.authorization.User
import org.apache.nifi.authorization.resource.Authorizable
import org.apache.nifi.authorization.resource.ResourceFactory
import org.apache.nifi.authorization.user.NiFiUser
import org.apache.nifi.authorization.user.NiFiUserDetails
import org.apache.nifi.authorization.user.StandardNiFiUser
import org.apache.nifi.controller.service.ControllerServiceProvider
import org.apache.nifi.reporting.Bulletin
import org.apache.nifi.reporting.BulletinRepository
import org.apache.nifi.web.api.dto.AccessPolicyDTO
import org.apache.nifi.web.api.dto.BulletinDTO
import org.apache.nifi.web.api.dto.DtoFactory
import org.apache.nifi.web.api.dto.EntityFactory
import org.apache.nifi.web.api.dto.RevisionDTO
import org.apache.nifi.web.api.dto.UserDTO
import org.apache.nifi.web.api.dto.UserGroupDTO
import org.apache.nifi.web.api.entity.BulletinEntity
import org.apache.nifi.web.api.entity.UserEntity
import org.apache.nifi.web.controller.ControllerFacade
import org.apache.nifi.web.dao.AccessPolicyDAO
import org.apache.nifi.web.dao.UserDAO
import org.apache.nifi.web.dao.UserGroupDAO
import org.apache.nifi.web.revision.DeleteRevisionTask
import org.apache.nifi.web.revision.ReadOnlyRevisionCallback
import org.apache.nifi.web.revision.RevisionClaim
import org.apache.nifi.web.revision.RevisionManager
import org.apache.nifi.web.revision.UpdateRevisionTask
import org.apache.nifi.web.security.token.NiFiAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Unroll

class StandardNiFiServiceFacadeSpec extends Specification {

    def setup() {
        final NiFiUser user = new StandardNiFiUser.Builder().identity("nifi-user").build();
        final NiFiAuthenticationToken auth = new NiFiAuthenticationToken(new NiFiUserDetails(user));
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    def cleanup() {
        SecurityContextHolder.getContext().setAuthentication(null);
    }

    @Ignore
    @Unroll
    def "CreateUser: isAuthorized: #isAuthorized"() {
        given:
        def userDao = Mock UserDAO
        def entityFactory = new EntityFactory()
        def dtoFactory = new DtoFactory()
        def authorizableLookup = Mock AuthorizableLookup
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setUserDAO userDao
        niFiServiceFacade.setEntityFactory entityFactory
        def newUser = new User.Builder().identifier(userDto.id).identity(userDto.identity).build()

        when:
        def userEntity = niFiServiceFacade.createUser(new Revision(0L, 'client-1', userDto.id), userDto)

        then:
        1 * userDao.createUser(_) >> newUser
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(parentAuthorizable, resource, isAuthorized, authorizationResult)
        0 * _
        userEntity != null
        if (isAuthorized) {
            assert userEntity?.component?.id == userDto.id
            assert userEntity?.component?.identity?.equals(userDto.identity)
            assert userEntity?.permissions?.canRead
            assert userEntity?.permissions?.canWrite
        } else {
            assert userEntity.component == null
        }


        where:
        userDto         | parentAuthorizable | resource                      | isAuthorized | authorizationResult
        createUserDTO() | null               | ResourceFactory.usersResource | true         | AuthorizationResult.approved()
        createUserDTO() | null               | ResourceFactory.usersResource | false        | AuthorizationResult.denied()
    }

    @Ignore
    @Unroll
    def "GetUser: isAuthorized: #isAuthorized"() {
        given:
        def userDao = Mock UserDAO
        def revisionManager = Mock RevisionManager
        def authorizableLookup = Mock AuthorizableLookup
        def dtoFactory = new DtoFactory()
        def entityFactory = new EntityFactory()
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setRevisionManager revisionManager
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setUserDAO userDao
        def requestedUser = new User.Builder().identifier(userDto.id).identity(userDto.identity).build()
        def exception = null
        def userEntity = null

        when:
        try {
            userEntity = niFiServiceFacade.getUser(userDto.id, true)
        } catch (AccessDeniedException e) {
            exception = e
        }

        then:
        1 * userDao.getUser(userDto.id) >> requestedUser
        1 * revisionManager.get(_, _) >> { String id, ReadOnlyRevisionCallback callback ->
            callback.withRevision(new Revision(1L, 'client1', 'root'))
        }
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getUsersResource(),
                isAuthorized, authorizationResult)
        0 * _

        assert userEntity != null
        if (isAuthorized) {
            assert userEntity.component?.id?.equals(userDto.id)
        } else {
            assert userEntity.component == null
        }

        where:
        userDto         | isAuthorized | authorizationResult
        createUserDTO() | true         | AuthorizationResult.approved()
        createUserDTO() | false        | AuthorizationResult.denied()
    }

    @Ignore
    @Unroll
    def "UpdateUser: isAuthorized: #isAuthorized, policy exists: #userExists"() {
        given:
        def userDao = Mock UserDAO
        def revisionManager = Mock RevisionManager
        def entityFactory = new EntityFactory()
        def dtoFactory = new DtoFactory()
        def authorizableLookup = Mock AuthorizableLookup
        def controllerFacade = Mock ControllerFacade
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setRevisionManager revisionManager
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setUserDAO userDao
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setControllerFacade controllerFacade
        def user = new User.Builder().identifier(userDto.id).identity(userDto.identity).build()

        when:
        def userEntityUpdateResult = niFiServiceFacade.updateUser(currentRevision, userDto)

        then:
        1 * userDao.hasUser(userDto.id) >> userExists
        if (!userExists) {
            1 * userDao.createUser(userDto) >> user
        } else {
            1 * controllerFacade.save()
            1 * userDao.updateUser(userDto) >> user
            1 * revisionManager.updateRevision(_, _, _) >> { RevisionClaim revisionClaim, NiFiUser niFiUser, UpdateRevisionTask callback ->
                callback.update()
            }
            1 * revisionManager.getRevision(currentRevision.componentId) >> currentRevision.incrementRevision(currentRevision.clientId)
        }
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getUsersResource(),
                isAuthorized, authorizationResult)
        0 * _
        userEntityUpdateResult != null
        def userEntity = userEntityUpdateResult?.result
        if (isAuthorized) {
            assert userEntity?.component?.id?.equals(userDto.id)
            assert userEntity?.getPermissions?.canRead
            assert userEntity?.getPermissions?.canWrite
        } else {
            assert userEntity.component == null
        }

        where:
        userExists | currentRevision                     | userDto         | isAuthorized | authorizationResult
        false      | new Revision(0L, 'client1', 'root') | createUserDTO() | true         | AuthorizationResult.approved()
        true       | new Revision(1L, 'client1', 'root') | createUserDTO() | true         | AuthorizationResult.approved()
        false      | new Revision(0L, 'client1', 'root') | createUserDTO() | false        | AuthorizationResult.denied()
        true       | new Revision(1L, 'client1', 'root') | createUserDTO() | false        | AuthorizationResult.denied()
    }

    @Ignore
    @Unroll
    def "DeleteUser: isAuthorized: #isAuthorized, user exists: #userExists"() {
        given:
        def userDao = Mock UserDAO
        def revisionManager = Mock RevisionManager
        def authorizableLookup = Mock AuthorizableLookup
        def dtoFactory = new DtoFactory()
        def entityFactory = new EntityFactory()
        def controllerFacade = Mock ControllerFacade
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setRevisionManager revisionManager
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setUserDAO userDao
        niFiServiceFacade.setControllerFacade controllerFacade
        def user = new User.Builder().identifier(userDto.id).identity(userDto.identity).build()

        when:
        def userEntity = niFiServiceFacade.deleteUser(currentRevision, userDto.id)

        then:
        if (userExists) {
            1 * userDao.getUser(userDto.id) >> user
            1 * userDao.deleteUser(userDto.id) >> user
        } else {
            1 * userDao.getUser(userDto.id) >> null
            1 * userDao.deleteUser(userDto.id) >> null
        }
        1 * controllerFacade.save()
        1 * revisionManager.deleteRevision(_, _, _) >> { RevisionClaim revisionClaim, NiFiUser nifiUser, DeleteRevisionTask task ->
            task.performTask()
        }
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.usersResource,
                isAuthorized, authorizationResult)
        0 * _
        userEntity?.component?.id == null
        if (userExists) {
            assert userEntity?.id?.equals(userDto.id)
        } else {
            assert userEntity?.id == null
        }

        where:
        userExists | currentRevision                       | userDto         | isAuthorized | authorizationResult
        true       | new Revision(1L, 'client1', 'user-1') | createUserDTO() | true         | AuthorizationResult.approved()
        false      | null                                  | createUserDTO() | true         | AuthorizationResult.approved()
        true       | new Revision(1L, 'client1', 'user-1') | createUserDTO() | false        | AuthorizationResult.denied()
        false      | null                                  | createUserDTO() | false        | AuthorizationResult.denied()
    }

    @Ignore
    @Unroll
    def "CreateUserGroup: isAuthorized: #isAuthorized"() {
        given:
        def userGroupDao = Mock UserGroupDAO
        def userDao = Mock UserDAO
        def revisionManager = Mock RevisionManager
        def controllerServiceProvider = Mock ControllerServiceProvider
        def entityFactory = new EntityFactory()
        def dtoFactory = new DtoFactory()
        dtoFactory.setControllerServiceProvider controllerServiceProvider
        dtoFactory.setEntityFactory entityFactory
        def authorizableLookup = Mock AuthorizableLookup
        def controllerFacade = Mock ControllerFacade
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setRevisionManager revisionManager
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setUserGroupDAO userGroupDao
        niFiServiceFacade.setUserDAO userDao
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setControllerFacade controllerFacade
        def newUserGroup = new Group.Builder().identifier(userGroupDto.id).name(userGroupDto.name).addUsers(userGroupDto.users.collect { it.id } as Set).build()
        def exception = null
        def userGroupEntity = null

        when:
        try {
            userGroupEntity = niFiServiceFacade.createUserGroup(new Revision(0L, 'client-1', userGroupDto.id), userGroupDto)
        } catch (AccessDeniedException e) {
            exception = e
        }

        then:
        1 * authorizableLookup.getUserGroupsAuthorizable() >>
                new SimpleAuthorizable(null, ResourceFactory.userGroupsResource, isAuthorized, authorizationResult.get(ResourceFactory.userGroupsResource))
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.usersResource, isAuthorized, authorizationResult.get(ResourceFactory.usersResource))
        1 * userGroupDao.createUserGroup(_) >> newUserGroup
        1 * userDao.getUser(_) >> { String userId ->
            def userEntity = userGroupDto.users.find { it.id.equals(userId) }?.component
            assert userEntity != null
            new User.Builder().identifier(userEntity.id).identity(userEntity.identity)
                    .addGroups(userEntity.groups.collect { it.id } as Set)
                    .build()
        }
        userGroupDto.users.size() * revisionManager.get(_, _) >> { String id, ReadOnlyRevisionCallback callback ->
            assert userGroupDto.users.collect { it.id }.contains(id)
            def revisionDTO = userGroupDto.users.find { it.id.equals(id) }.revision
            callback.withRevision new Revision(revisionDTO.version, revisionDTO.clientId, id)
        }
        0 * _

        assert userGroupEntity != null
        if (isAuthorized) {
            assert userGroupEntity?.component?.id == userGroupDto.id
            assert userGroupEntity?.component?.users?.equals(userGroupDto.users)
            assert userGroupEntity?.permissions?.canRead
            assert userGroupEntity?.permissions?.canWrite
        } else {
            assert userGroupEntity?.component == null
        }


        where: // TODO add more use cases, specifically with varied authorization results, and the assertions to check them, to all spec methods that use AuthorizationResult
        userGroupDto         | isAuthorized | authorizationResult
        createUserGroupDTO() | true         | [(ResourceFactory.userGroupsResource): AuthorizationResult.approved(), (ResourceFactory.usersResource): AuthorizationResult.approved()]
        createUserGroupDTO() | false        | [(ResourceFactory.userGroupsResource): AuthorizationResult.denied(), (ResourceFactory.usersResource): AuthorizationResult.denied()]
    }

    @Ignore
    @Unroll
    def "GetUserGroup: isAuthorized: #isAuthorized"() {
        given:
        def userGroupDao = Mock UserGroupDAO
        def userDao = Mock UserDAO
        def revisionManager = Mock RevisionManager
        def authorizableLookup = Mock AuthorizableLookup
        def dtoFactory = new DtoFactory()
        def entityFactory = new EntityFactory()
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setRevisionManager revisionManager
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setUserGroupDAO userGroupDao
        niFiServiceFacade.setUserDAO userDao
        def requestedUserGroup = new Group.Builder().identifier(userGroupDto.id).name(userGroupDto.name)
                .addUsers(userGroupDto.users.collect { it.id } as Set).build()
        def exception = null
        def userGroupEntity = null

        when:
        try {
            userGroupEntity = niFiServiceFacade.getUserGroup(userGroupDto.id, true)
        } catch (AccessDeniedException e) {
            exception = e
        }

        then:
        1 * userGroupDao.getUserGroup(userGroupDto.id) >> requestedUserGroup
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.usersResource, isAuthorized, authorizationResult)
        1 * authorizableLookup.getUserGroupsAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getUserGroupsResource(),
                isAuthorized, authorizationResult)
        _ * revisionManager.get(_, _) >> { String id, ReadOnlyRevisionCallback callback ->
            callback.withRevision(new Revision(1L, 'client1', 'root'))
        }
        1 * userDao.getUser(_) >> { String userId ->
            def userEntity = userGroupDto.users.find { it.id.equals(userId) }?.component
            assert userEntity != null
            new User.Builder().identifier(userEntity.id).identity(userEntity.identity).build()
        }
        0 * _

        assert userGroupEntity != null
        if (isAuthorized) {
            assert userGroupEntity?.component?.id?.equals(userGroupDto.id)
        } else {
            assert userGroupEntity.component == null
        }

        where:
        userGroupDto                                                               | isAuthorized | authorizationResult
        new UserGroupDTO(id: '1', name: 'test group', users: [createUserEntity()]) | true         | AuthorizationResult.approved()
        new UserGroupDTO(id: '1', name: 'test group', users: [createUserEntity()]) | false        | AuthorizationResult.denied()
    }

    @Ignore
    @Unroll
    def "UpdateUserGroup: isAuthorized: #isAuthorized, userGroupExists exists: #userGroupExists"() {
        given:
        def userGroupDao = Mock UserGroupDAO
        def userDao = Mock UserDAO
        def revisionManager = Mock RevisionManager
        def entityFactory = new EntityFactory()
        def dtoFactory = new DtoFactory()
        def authorizableLookup = Mock AuthorizableLookup
        def controllerFacade = Mock ControllerFacade
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setRevisionManager revisionManager
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setUserGroupDAO userGroupDao
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setControllerFacade controllerFacade
        niFiServiceFacade.setUserDAO userDao
        def userGroup = new Group.Builder().identifier(userGroupDto.id).name(userGroupDto.name)
                .addUsers(userGroupDto.users.collect { it.id } as Set).build()
        def userGroupsEntityUpdateResult = null
        def exception = null

        when:
        try {
            userGroupsEntityUpdateResult = niFiServiceFacade.updateUserGroup(currentRevision, userGroupDto)
        } catch (AccessDeniedException e) {
            exception = e
        }

        then:
        1 * userGroupDao.hasUserGroup(userGroupDto.id) >> userGroupExists
        if (!userGroupExists) {
            1 * userGroupDao.createUserGroup(userGroupDto) >> userGroup
            1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getUsersResource(),
                    isAuthorized, authorizationResult.get(ResourceFactory.getUsersResource()))
        } else {
            1 * controllerFacade.save()
            1 * userGroupDao.updateUserGroup(userGroupDto) >> userGroup
            1 * revisionManager.updateRevision(_, _, _) >> { RevisionClaim revisionClaim, NiFiUser niFiUser, UpdateRevisionTask callback ->
                callback.update()
            }
            1 * revisionManager.getRevision(currentRevision.componentId) >> currentRevision.incrementRevision(currentRevision.clientId)
            1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.usersResource,
                    isAuthorized, authorizationResult.get(ResourceFactory.usersResource))
        }
        1 * authorizableLookup.getUserGroupsAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.userGroupsResource,
                isAuthorized, authorizationResult.get(ResourceFactory.userGroupsResource))
        _ * revisionManager.get(_, _) >> { String id, ReadOnlyRevisionCallback callback ->
            callback.withRevision(new Revision(1L, 'client1', 'root'))
        }
        1 * userDao.getUser(_) >> { String userId ->
            def userEntity = userGroupDto.users.find { it.id.equals(userId) }?.component
            assert userEntity != null
            new User.Builder().identifier(userEntity.id).identity(userEntity.identity).build()
        }
        0 * _
        def userGroupEntity = userGroupsEntityUpdateResult?.result

        assert userGroupEntity != null
        if (isAuthorized) {
            assert userGroupEntity?.component?.id?.equals(userGroupDto.id)
            assert userGroupEntity?.getPermissions?.canRead
            assert userGroupEntity?.getPermissions?.canWrite
        } else {
            assert userGroupEntity.component == null
        }

        where:
        userGroupExists | currentRevision                     | userGroupDto                                                               | isAuthorized |
                authorizationResult
        false           | new Revision(0L, 'client1', 'root') | new UserGroupDTO(id: '1', name: 'test group', users: [createUserEntity()]) | true         |
                [(ResourceFactory.userGroupsResource): AuthorizationResult.approved(), (ResourceFactory.usersResource): AuthorizationResult.approved()]
        true            | new Revision(1L, 'client1', 'root') | new UserGroupDTO(id: '1', name: 'test group', users: [createUserEntity()]) | true         |
                [(ResourceFactory.userGroupsResource): AuthorizationResult.approved(), (ResourceFactory.usersResource): AuthorizationResult.approved()]
        false           | new Revision(0L, 'client1', 'root') | new UserGroupDTO(id: '1', name: 'test group', users: [createUserEntity()]) | false        |
                [(ResourceFactory.userGroupsResource): AuthorizationResult.denied(), (ResourceFactory.usersResource): AuthorizationResult.denied()]
        true            | new Revision(1L, 'client1', 'root') | new UserGroupDTO(id: '1', name: 'test group', users: [createUserEntity()]) | false        |
                [(ResourceFactory.userGroupsResource): AuthorizationResult.denied(), (ResourceFactory.usersResource): AuthorizationResult.denied()]
    }

    @Ignore
    @Unroll
    def "DeleteUserGroup: isAuthorized: #isAuthorized, userGroup exists: #userGroupExists"() {
        given:
        def userGroupDao = Mock UserGroupDAO
        def userDao = Mock UserDAO
        def revisionManager = Mock RevisionManager
        def authorizableLookup = Mock AuthorizableLookup
        def dtoFactory = new DtoFactory()
        def entityFactory = new EntityFactory()
        def controllerFacade = Mock ControllerFacade
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setRevisionManager revisionManager
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setUserGroupDAO userGroupDao
        niFiServiceFacade.setControllerFacade controllerFacade
        niFiServiceFacade.setUserDAO userDao
        def userGroup = new Group.Builder().identifier(userGroupDto.id).name(userGroupDto.name)
                .addUsers(userGroupDto.users.collect { it.id } as Set).build()
        def userGroupEntity = null
        def exception = null

        when:
        try {
            userGroupEntity = niFiServiceFacade.deleteUserGroup(currentRevision, userGroupDto.id)
        } catch (AccessDeniedException e) {
            exception = e
        }

        then:
        if (userGroupExists) {
            1 * userGroupDao.getUserGroup(userGroupDto.id) >> userGroup
            1 * userGroupDao.deleteUserGroup(userGroupDto.id) >> userGroup
            1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getUsersResource(),
                    isAuthorized, authorizationResult.get(ResourceFactory.getUsersResource()))
        } else {
            1 * userGroupDao.getUserGroup(userGroupDto.id) >> null
            1 * userGroupDao.deleteUserGroup(userGroupDto.id) >> null
        }
        1 * authorizableLookup.getUserGroupsAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.userGroupsResource,
                isAuthorized, authorizationResult.get(ResourceFactory.userGroupsResource))
        1 * revisionManager.deleteRevision(_, _, _) >> { RevisionClaim revisionClaim, NiFiUser nifiUser, DeleteRevisionTask task ->
            task.performTask()
        }
        1 * controllerFacade.save()
        _ * revisionManager.get(_, _) >> { String id, ReadOnlyRevisionCallback callback ->
            callback.withRevision(new Revision(1L, 'client1', 'root'))
        }
        if (userGroupExists) {
            1 * userDao.getUser(_) >> { String userId ->
                def userEntity = userGroupDto.users.find { it.id.equals(userId) }?.component
                assert userEntity != null
                new User.Builder().identifier(userEntity.id).identity(userEntity.identity).build()
            }
        }
        0 * _
        userGroupEntity?.component?.id == null
        if (userGroupExists) {
            assert userGroupEntity?.id?.equals(userGroupDto.id)
        } else {
            assert userGroupEntity?.id == null
        }

        where:
        userGroupExists | currentRevision                     | userGroupDto                                                               | isAuthorized |
                authorizationResult
        true            | new Revision(1L, 'client1', 'root') | new UserGroupDTO(id: '1', name: 'test group', users: [createUserEntity()]) | true         |
                [(ResourceFactory.userGroupsResource): AuthorizationResult.approved(), (ResourceFactory.usersResource): AuthorizationResult.approved()]
        false           | null                                | new UserGroupDTO(id: '1', name: 'test group', users: [createUserEntity()]) | true         |
                [(ResourceFactory.userGroupsResource): AuthorizationResult.approved(), (ResourceFactory.usersResource): AuthorizationResult.approved()]
        true            | new Revision(1L, 'client1', 'root') | new UserGroupDTO(id: '1', name: 'test group', users: [createUserEntity()]) | false        |
                [(ResourceFactory.userGroupsResource): AuthorizationResult.denied(), (ResourceFactory.usersResource): AuthorizationResult.denied()]
        false           | null                                | new UserGroupDTO(id: '1', name: 'test group', users: [createUserEntity()]) | false        |
                [(ResourceFactory.userGroupsResource): AuthorizationResult.denied(), (ResourceFactory.usersResource): AuthorizationResult.denied()]
    }

    @Ignore
    @Unroll
    def "CreateAccessPolicy: #isAuthorized"() {
        given:
        def accessPolicyDao = Mock AccessPolicyDAO
        def userDao = Mock UserDAO
        def revisionManager = Mock RevisionManager
        def entityFactory = new EntityFactory()
        def dtoFactory = new DtoFactory()
        dtoFactory.setEntityFactory entityFactory
        def authorizableLookup = Mock AuthorizableLookup
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setAccessPolicyDAO accessPolicyDao
        niFiServiceFacade.setUserDAO userDao
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setRevisionManager revisionManager
        def builder = new AccessPolicy.Builder().identifier(accessPolicyDto.id).resource(accessPolicyDto.resource)
                .addUsers(accessPolicyDto.users.collect { it.id } as Set)
                .addGroups(accessPolicyDto.userGroups.collect { it.id } as Set)
        if (accessPolicyDto.canRead) {
            builder.addAction(RequestAction.READ)
        }
        if (accessPolicyDto.canWrite) {
            builder.addAction(RequestAction.WRITE)
        }
        def newAccessPolicy = builder.build()
        def accessPolicyEntity = null
        def exception = null

        when:
        try {
            accessPolicyEntity = niFiServiceFacade.createAccessPolicy(new Revision(0L, 'client-1', accessPolicyDto.id), accessPolicyDto)
        } catch (AccessDeniedException e) {
            exception = e
        }

        then:
        1 * accessPolicyDao.createAccessPolicy(accessPolicyDto) >> newAccessPolicy
        1 * authorizableLookup.getAccessPolicyAuthorizable(accessPolicyDto.id) >> new SimpleAuthorizable(null, ResourceFactory.getPolicyResource(accessPolicyDto.id),
                isAuthorized, authorizationResult)
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getUsersResource(),
                isAuthorized, authorizationResult)
        1 * userDao.getUser(_) >> { String userId ->
            def userEntity = accessPolicyDto.users.find { it.id.equals(userId) }?.component
            assert userEntity != null
            new User.Builder().identifier(userEntity.id).identity(userEntity.identity).build()
        }
        1 * revisionManager.get(_, _) >> { String id, ReadOnlyRevisionCallback callback ->
            callback.withRevision(new Revision(1L, 'client1', 'root'))
        }
        0 * _

        assert accessPolicyEntity != null
        if (isAuthorized) {
            assert accessPolicyEntity?.component?.id?.equals(accessPolicyDto.id)
            assert accessPolicyEntity?.permissions?.canRead
            assert accessPolicyEntity?.permissions?.canWrite
        } else {
            assert accessPolicyEntity.component == null
        }

        where:
        accessPolicyDto                                                                                                             | isAuthorized | authorizationResult
        new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: [createUserEntity()], canRead: true) | true         | AuthorizationResult.approved()
        new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: [createUserEntity()], canRead: true) | false        | AuthorizationResult.denied()
    }

    @Ignore
    @Unroll
    def "GetAccessPolicy: isAuthorized: #isAuthorized"() {
        given:
        def accessPolicyDao = Mock AccessPolicyDAO
        def userDao = Mock UserDAO
        def revisionManager = Mock RevisionManager
        def authorizableLookup = Mock AuthorizableLookup
        def dtoFactory = new DtoFactory()
        def entityFactory = new EntityFactory()
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setRevisionManager revisionManager
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setAccessPolicyDAO accessPolicyDao
        niFiServiceFacade.setUserDAO userDao
        def builder = new AccessPolicy.Builder().identifier(accessPolicyDto.id).resource(accessPolicyDto.resource)
                .addUsers(accessPolicyDto.users.collect { it.id } as Set)
                .addGroups(accessPolicyDto.userGroups.collect { it.id } as Set)
        if (accessPolicyDto.canRead) {
            builder.addAction(RequestAction.READ)
        }
        if (accessPolicyDto.canWrite) {
            builder.addAction(RequestAction.WRITE)
        }
        def requestedAccessPolicy = builder.build()
        def exception = null
        def accessPolicyEntity = null

        when:
        try {
            accessPolicyEntity = niFiServiceFacade.getAccessPolicy(accessPolicyDto.id)
        } catch (AccessDeniedException e) {
            exception = e
        }

        then:
        1 * accessPolicyDao.getAccessPolicy(accessPolicyDto.id) >> requestedAccessPolicy
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getUsersResource(),
                isAuthorized, authorizationResult)
        _ * revisionManager.get(_, _) >> { String id, ReadOnlyRevisionCallback callback ->
            callback.withRevision(new Revision(1L, 'client1', 'root'))
        }
        1 * authorizableLookup.getAccessPolicyAuthorizable(accessPolicyDto.id) >> new SimpleAuthorizable(null, ResourceFactory.getPolicyResource(accessPolicyDto.id),
                isAuthorized, authorizationResult)
        1 * userDao.getUser(_) >> { String userId ->
            def userEntity = accessPolicyDto.users.find { it.id.equals(userId) }?.component
            assert userEntity != null
            new User.Builder().identifier(userEntity.id).identity(userEntity.identity).build()
        }
        0 * _

        assert accessPolicyEntity != null
        if (isAuthorized) {
            assert accessPolicyEntity?.component?.id?.equals(accessPolicyDto.id)
        } else {
            assert accessPolicyEntity.component == null
        }

        where:
        accessPolicyDto                                                                                                             | isAuthorized | authorizationResult
        new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: [createUserEntity()], canRead: true) | true         | AuthorizationResult.approved()
        new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: [createUserEntity()], canRead: true) | false        | AuthorizationResult.denied()
    }

    @Ignore
    @Unroll
    def "UpdateAccessPolicy: isAuthorized: #isAuthorized, policy exists: #hasPolicy"() {
        given:
        def accessPolicyDao = Mock AccessPolicyDAO
        def userDao = Mock UserDAO
        def revisionManager = Mock RevisionManager
        def entityFactory = new EntityFactory()
        def dtoFactory = new DtoFactory()
        def authorizableLookup = Mock AuthorizableLookup
        def controllerFacade = Mock ControllerFacade
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setRevisionManager revisionManager
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setAccessPolicyDAO accessPolicyDao
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setControllerFacade controllerFacade
        niFiServiceFacade.setUserDAO userDao
        def builder = new AccessPolicy.Builder().identifier(accessPolicyDto.id).resource(accessPolicyDto.resource)
                .addUsers(accessPolicyDto.users.collect { it.id } as Set)
                .addGroups(accessPolicyDto.userGroups.collect { it.id } as Set)
        if (accessPolicyDto.canRead) {
            builder.addAction(RequestAction.READ)
        }
        if (accessPolicyDto.canWrite) {
            builder.addAction(RequestAction.WRITE)
        }
        def accessPolicy = builder.build()
        def accessPolicyEntityUpdateResult = null
        def exception = null

        when:
        try {
            accessPolicyEntityUpdateResult = niFiServiceFacade.updateAccessPolicy(currentRevision, accessPolicyDto)
        } catch (AccessDeniedException e) {
            exception = e
        }

        then:
        1 * accessPolicyDao.hasAccessPolicy(accessPolicyDto.id) >> hasPolicy
        if (!hasPolicy) {
            1 * accessPolicyDao.createAccessPolicy(accessPolicyDto) >> accessPolicy
        } else {
            1 * controllerFacade.save()
            1 * accessPolicyDao.updateAccessPolicy(accessPolicyDto) >> accessPolicy
            1 * revisionManager.updateRevision(_, _, _) >> { RevisionClaim revisionClaim, NiFiUser niFiUser, UpdateRevisionTask callback ->
                callback.update()
            }
            1 * revisionManager.getRevision(currentRevision.componentId) >> currentRevision.incrementRevision(currentRevision.clientId)
        }
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getUsersResource(),
                isAuthorized, authorizationResult)
        1 * authorizableLookup.getAccessPolicyAuthorizable(accessPolicyDto.id) >> new SimpleAuthorizable(null, ResourceFactory.getPolicyResource(accessPolicyDto.id),
                isAuthorized, authorizationResult)
        1 * userDao.getUser(_) >> { String userId ->
            def userEntity = accessPolicyDto.users.find { it.id.equals(userId) }?.component
            assert userEntity != null
            new User.Builder().identifier(userEntity.id).identity(userEntity.identity).build()
        }
        1 * revisionManager.get(_, _) >> { String id, ReadOnlyRevisionCallback callback ->
            callback.withRevision(new Revision(1L, 'client1', 'root'))
        }
        0 * _
        def accessPolicyEntity = accessPolicyEntityUpdateResult?.result

        assert accessPolicyEntity != null
        if (isAuthorized) {
            assert accessPolicyEntity?.component?.id?.equals(accessPolicyDto.id)
            assert accessPolicyEntity?.getPermissions?.canRead
            assert accessPolicyEntity?.getPermissions?.canWrite
        } else {
            assert accessPolicyEntity.component == null
        }

        where:
        hasPolicy | currentRevision                     | accessPolicyDto                                                                                                             | isAuthorized |
                authorizationResult
        false     | new Revision(0L, 'client1', 'root') | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: [createUserEntity()], canRead: true) | true         |
                AuthorizationResult.approved()
        true      | new Revision(1L, 'client1', 'root') | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: [createUserEntity()], canRead: true) | true         |
                AuthorizationResult.approved()
        false     | new Revision(0L, 'client1', 'root') | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: [createUserEntity()], canRead: true) | false        |
                AuthorizationResult.denied()
        true      | new Revision(1L, 'client1', 'root') | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: [createUserEntity()], canRead: true) | false        |
                AuthorizationResult.denied()
    }

    @Ignore
    @Unroll
    def "DeleteAccessPolicy: isAuthorized: #isAuthorized, hasPolicy: #hasPolicy"() {
        given:
        def accessPolicyDao = Mock AccessPolicyDAO
        def userDao = Mock UserDAO
        def revisionManager = Mock RevisionManager
        def authorizableLookup = Mock AuthorizableLookup
        def dtoFactory = new DtoFactory()
        def entityFactory = new EntityFactory()
        def controllerFacade = Mock ControllerFacade
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setRevisionManager revisionManager
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setAccessPolicyDAO accessPolicyDao
        niFiServiceFacade.setControllerFacade controllerFacade
        niFiServiceFacade.setUserDAO userDao
        def builder = new AccessPolicy.Builder()
        builder.identifier(accessPolicyDto.id).resource(accessPolicyDto.resource)
                .addUsers(accessPolicyDto.users.collect { it.id } as Set)
                .addGroups(accessPolicyDto.userGroups.collect { it.id } as Set)
        if (accessPolicyDto.canRead) {
            builder.addAction(RequestAction.READ)
        }
        if (accessPolicyDto.canWrite) {
            builder.addAction(RequestAction.WRITE)
        }
        def accessPolicy = builder.build()
        def accessPolicyEntity = null
        def exception = null

        when:
        try {
            accessPolicyEntity = niFiServiceFacade.deleteAccessPolicy(currentRevision, accessPolicyDto.id)
        } catch (AccessDeniedException e) {
            exception = e
        }

        then:
        if (hasPolicy) {
            1 * accessPolicyDao.getAccessPolicy(accessPolicyDto.id) >> accessPolicy
            1 * accessPolicyDao.deleteAccessPolicy(accessPolicyDto.id) >> accessPolicy
            1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.usersResource,
                    isAuthorized, authorizationResult)
        } else {
            1 * accessPolicyDao.getAccessPolicy(accessPolicyDto.id) >> null
            1 * accessPolicyDao.deleteAccessPolicy(accessPolicyDto.id) >> null
        }
        1 * authorizableLookup.getAccessPolicyAuthorizable(accessPolicyDto.id) >> new SimpleAuthorizable(null, ResourceFactory.getPolicyResource(accessPolicyDto.id),
                isAuthorized, authorizationResult)
        1 * revisionManager.deleteRevision(_, _, _) >> { RevisionClaim revisionClaim, NiFiUser nifiUser, DeleteRevisionTask task ->
            task.performTask()
        }
        1 * controllerFacade.save()
        _ * revisionManager.get(_, _) >> { String id, ReadOnlyRevisionCallback callback ->
            callback.withRevision(new Revision(1L, 'client1', 'root'))
        }
        if (hasPolicy) {
            1 * userDao.getUser(_) >> { String userId ->
                def userEntity = accessPolicyDto.users.find { it.id.equals(userId) }?.component
                assert userEntity != null
                new User.Builder().identifier(userEntity.id).identity(userEntity.identity).build()
            }
        }
        0 * _

        assert accessPolicyEntity != null
        if (hasPolicy) {
            assert accessPolicyEntity?.id?.equals(accessPolicyDto.id)
        } else {
            assert accessPolicyEntity?.id == null
        }

        where:
        hasPolicy | currentRevision                     | accessPolicyDto                                                                                                             | isAuthorized |
                authorizationResult
        true      | new Revision(1L, 'client1', 'root') | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: [createUserEntity()], canRead: true) | true         |
                AuthorizationResult.approved()
        false     | null                                | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: [createUserEntity()], canRead: true) | true         |
                AuthorizationResult.approved()
        true      | new Revision(1L, 'client1', 'root') | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: [createUserEntity()], canRead: true) | false        |
                AuthorizationResult.denied()
        false     | null                                | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: [createUserEntity()], canRead: true) | false        |
                AuthorizationResult.denied()
    }


    def "CreateBulletin Successfully"() {
        given:

        def entityFactory = new EntityFactory()
        def dtoFactory = new DtoFactory()
        dtoFactory.setEntityFactory entityFactory
        def authorizableLookup = Mock AuthorizableLookup
        def controllerFacade = Mock ControllerFacade
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        def bulletinRepository = Mock BulletinRepository
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setControllerFacade controllerFacade
        niFiServiceFacade.setBulletinRepository bulletinRepository

        def bulletinDto = new BulletinDTO()
        bulletinDto.category = "SYSTEM"
        bulletinDto.message = "test system message"
        bulletinDto.level = "WARN"
        def bulletinEntity
        def retBulletinEntity = new BulletinEntity()
        retBulletinEntity.bulletin = bulletinDto

        when:

        bulletinEntity = niFiServiceFacade.createBulletin(bulletinDto,true)


        then:
        1 * bulletinRepository.addBulletin(_ as Bulletin)
        bulletinEntity
        bulletinEntity.bulletin.message == bulletinDto.message


    }

    private UserGroupDTO createUserGroupDTO() {
        new UserGroupDTO(id: 'group-1', name: 'test group', users: [createUserEntity()] as Set)
    }

    private UserEntity createUserEntity() {
        new UserEntity(id: 'user-1', component: createUserDTO(), revision: createRevisionDTO())
    }

    private UserDTO createUserDTO() {
        new UserDTO(id: 'user-1', identity: 'user-1')
    }

    private RevisionDTO createRevisionDTO() {
        new RevisionDTO(version: 0L, clientId: 'client-1', lastModifier: 'user-1')
    }

    private class SimpleAuthorizable implements Authorizable {
        final private Authorizable parentAuthorizable
        final private Resource resource
        final private boolean isAuthorized
        final private AuthorizationResult authorizationResult;

        SimpleAuthorizable(Authorizable parentAuthorizable, Resource resource, boolean isAuthorized, AuthorizationResult authorizationResult) {
            this.parentAuthorizable = parentAuthorizable
            this.resource = resource
            this.isAuthorized = isAuthorized
            this.authorizationResult = authorizationResult
        }

        @Override
        Authorizable getParentAuthorizable() {
            return parentAuthorizable
        }

        @Override
        Resource getResource() {
            return resource
        }

        @Override
        boolean isAuthorized(Authorizer authorzr, RequestAction action, NiFiUser user) {
            return isAuthorized
        }

        @Override
        AuthorizationResult checkAuthorization(Authorizer authorzr, RequestAction action, NiFiUser user) {
            return authorizationResult
        }

        @Override
        void authorize(Authorizer authorzr, RequestAction action, NiFiUser user) throws AccessDeniedException {
            if (!isAuthorized) {
                throw new AccessDeniedException("test exception, access denied")
            }
        }
    }
}