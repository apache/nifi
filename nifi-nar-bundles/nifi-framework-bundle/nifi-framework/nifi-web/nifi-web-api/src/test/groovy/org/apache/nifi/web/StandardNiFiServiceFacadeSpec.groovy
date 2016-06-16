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
import org.apache.nifi.authorization.AuthorizationResult
import org.apache.nifi.authorization.Authorizer
import org.apache.nifi.authorization.Group
import org.apache.nifi.authorization.RequestAction
import org.apache.nifi.authorization.Resource
import org.apache.nifi.authorization.User
import org.apache.nifi.authorization.resource.Authorizable
import org.apache.nifi.authorization.resource.ResourceFactory
import org.apache.nifi.authorization.user.NiFiUser
import org.apache.nifi.controller.service.ControllerServiceProvider
import org.apache.nifi.web.api.dto.*
import org.apache.nifi.web.controller.ControllerFacade
import org.apache.nifi.web.dao.AccessPolicyDAO
import org.apache.nifi.web.dao.UserDAO
import org.apache.nifi.web.dao.UserGroupDAO
import org.apache.nifi.web.revision.DeleteRevisionTask
import org.apache.nifi.web.revision.ReadOnlyRevisionCallback
import org.apache.nifi.web.revision.RevisionClaim
import org.apache.nifi.web.revision.RevisionManager
import org.apache.nifi.web.revision.UpdateRevisionTask
import spock.lang.Specification
import spock.lang.Unroll

class StandardNiFiServiceFacadeSpec extends Specification {

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
        def userEntity = niFiServiceFacade.createUser(new Revision(0L, 'client-1'), userDto)

        then:
        1 * userDao.createUser(_) >> newUser
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(parentAuthorizable, resource, isAuthorized, authorizationResult)
        0 * _
        userEntity != null
        if (isAuthorized) {
            assert userEntity?.component?.id == userDto.id
            assert userEntity?.component?.identity?.equals(userDto.identity)
            assert userEntity?.accessPolicy?.canRead
            assert userEntity?.accessPolicy?.canWrite
        } else {
            assert userEntity.component == null
        }


        where:
        userDto                                  | parentAuthorizable | resource                      | isAuthorized | authorizationResult
        new UserDTO(id: '1', identity: 'a user') | null               | ResourceFactory.usersResource | true         | AuthorizationResult.approved()
        new UserDTO(id: '1', identity: 'a user') | null               | ResourceFactory.usersResource | false        | AuthorizationResult.denied()
    }

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
            userEntity = niFiServiceFacade.getUser(userDto.id)
        } catch (AccessDeniedException e) {
            exception = e
        }

        then:
        if (isAuthorized) {
            1 * userDao.getUser(userDto.id) >> requestedUser
        }
        1 * revisionManager.get(_, _) >> { String id, ReadOnlyRevisionCallback callback ->
            callback.withRevision(new Revision(1L, 'client1', 'root'))
        }
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getUsersResource(),
                isAuthorized, authorizationResult)
        0 * _
        if (isAuthorized) {
            assert userEntity?.component?.id?.equals(userDto.id)
        } else {
            assert exception instanceof AccessDeniedException
        }

        where:
        userDto                                  | isAuthorized | authorizationResult
        new UserDTO(id: '1', identity: 'a user') | true         | AuthorizationResult.approved()
        new UserDTO(id: '1', identity: 'a user') | false        | AuthorizationResult.denied()
    }

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
        }
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getPolicyResource(userDto.id),
                isAuthorized, authorizationResult)
        0 * _
        userEntityUpdateResult != null
        def userEntity = userEntityUpdateResult?.result
        if (isAuthorized) {
            assert userEntity?.component?.id?.equals(userDto.id)
            assert userEntity?.accessPolicy?.canRead
            assert userEntity?.accessPolicy?.canWrite
        } else {
            assert userEntity.component == null
        }

        where:
        userExists | currentRevision                     | userDto                                  | isAuthorized | authorizationResult
        false      | new Revision(0L, 'client1', 'root') | new UserDTO(id: '1', identity: 'a user') | true         | AuthorizationResult.approved()
        true       | new Revision(1L, 'client1', 'root') | new UserDTO(id: '1', identity: 'a user') | true         | AuthorizationResult.approved()
        false      | new Revision(0L, 'client1', 'root') | new UserDTO(id: '1', identity: 'a user') | false        | AuthorizationResult.denied()
        true       | new Revision(1L, 'client1', 'root') | new UserDTO(id: '1', identity: 'a user') | false        | AuthorizationResult.denied()
    }

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
        1 * authorizableLookup.getUsersAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getPolicyResource(userDto.id),
                isAuthorized, authorizationResult)
        0 * _
        userEntity?.component?.id == null
        if (userExists) {
            assert userEntity?.id?.equals(userDto.id)
        } else {
            assert userEntity?.id == null
        }

        where:
        userExists | currentRevision                     | userDto                                  | isAuthorized | authorizationResult
        true       | new Revision(1L, 'client1', 'root') | new UserDTO(id: '1', identity: 'a user') | true         | AuthorizationResult.approved()
        false      | null                                | new UserDTO(id: '1', identity: 'a user') | true         | AuthorizationResult.approved()
        true       | new Revision(1L, 'client1', 'root') | new UserDTO(id: '1', identity: 'a user') | false        | AuthorizationResult.denied()
        false      | null                                | new UserDTO(id: '1', identity: 'a user') | false        | AuthorizationResult.denied()
    }

    @Unroll
    def "CreateUserGroup: isAuthorized: #isAuthorized"() {
        given:
        def userGroupDao = Mock UserGroupDAO
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
        niFiServiceFacade.setEntityFactory entityFactory
        niFiServiceFacade.setControllerFacade controllerFacade
        def newUserGroup = new Group.Builder().identifier(userGroupDto.id).name(userGroupDto.name).addUsers(userGroupDto.users).build()

        when:
        def userGroupEntity = niFiServiceFacade.createUserGroup(new Revision(0L, 'client-1'), userGroupDto)

        then:
        1 * userGroupDao.createUserGroup(_) >> newUserGroup
        1 * authorizableLookup.getUserGroupsAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.userGroupsResource, isAuthorized, authorizationResult)
        0 * _
        userGroupEntity != null
        if (isAuthorized) {
            assert userGroupEntity?.component?.id == userGroupDto.id
            assert userGroupEntity?.component?.users?.equals(userGroupDto.users)
            assert userGroupEntity?.accessPolicy?.canRead
            assert userGroupEntity?.accessPolicy?.canWrite
        } else {
            assert userGroupEntity.component == null
        }


        where:
        userGroupDto                                                         | isAuthorized | authorizationResult
        new UserGroupDTO(id: '1', name: 'test group', users: ['first user']) | true         | AuthorizationResult.approved()
        new UserGroupDTO(id: '1', name: 'test group', users: ['first user']) | false        | AuthorizationResult.denied()
    }

    @Unroll
    def "GetUserGroup: isAuthorized: #isAuthorized"() {
        given:
        def userGroupDao = Mock UserGroupDAO
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
        def requestedUserGroup = new Group.Builder().identifier(userGroupDto.id).name(userGroupDto.name).addUsers(userGroupDto.users).build()
        def exception = null
        def userGroupEntity = null

        when:
        try {
            userGroupEntity = niFiServiceFacade.getUserGroup(userGroupDto.id)
        } catch (AccessDeniedException e) {
            exception = e
        }

        then:
        if (isAuthorized) {
            1 * userGroupDao.getUserGroup(userGroupDto.id) >> requestedUserGroup
        }
        1 * revisionManager.get(_, _) >> { String id, ReadOnlyRevisionCallback callback ->
            callback.withRevision(new Revision(1L, 'client1', 'root'))
        }
        1 * authorizableLookup.getUserGroupsAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getUserGroupsResource(),
                isAuthorized, authorizationResult)
        0 * _
        if (isAuthorized) {
            assert userGroupEntity?.component?.id?.equals(userGroupDto.id)
        } else {
            assert exception instanceof AccessDeniedException
        }

        where:
        userGroupDto                                                         | isAuthorized | authorizationResult
        new UserGroupDTO(id: '1', name: 'test group', users: ['first user']) | true         | AuthorizationResult.approved()
        new UserGroupDTO(id: '1', name: 'test group', users: ['first user']) | false        | AuthorizationResult.denied()
    }

    @Unroll
    def "UpdateUserGroup: isAuthorized: #isAuthorized, policy exists: #hasPolicy"() {
        given:
        def userGroupDao = Mock UserGroupDAO
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
        def userGroup = new Group.Builder().identifier(userGroupDto.id).name(userGroupDto.name).addUsers(userGroupDto.users).build()

        when:
        def userGroupsEntityUpdateResult = niFiServiceFacade.updateUserGroup(currentRevision, userGroupDto)

        then:
        1 * userGroupDao.hasUserGroup(userGroupDto.id) >> hasPolicy
        if (!hasPolicy) {
            1 * userGroupDao.createUserGroup(userGroupDto) >> userGroup
        } else {
            1 * controllerFacade.save()
            1 * userGroupDao.updateUserGroup(userGroupDto) >> userGroup
            1 * revisionManager.updateRevision(_, _, _) >> { RevisionClaim revisionClaim, NiFiUser niFiUser, UpdateRevisionTask callback ->
                callback.update()
            }
        }
        1 * authorizableLookup.getUserGroupsAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getPolicyResource(userGroupDto.id),
                isAuthorized, authorizationResult)
        0 * _
        userGroupsEntityUpdateResult != null
        def userGroupEntity = userGroupsEntityUpdateResult?.result
        if (isAuthorized) {
            assert userGroupEntity?.component?.id?.equals(userGroupDto.id)
            assert userGroupEntity?.accessPolicy?.canRead
            assert userGroupEntity?.accessPolicy?.canWrite
        } else {
            assert userGroupEntity.component == null
        }

        where:
        hasPolicy | currentRevision                     | userGroupDto                                                         | isAuthorized | authorizationResult
        false     | new Revision(0L, 'client1', 'root') | new UserGroupDTO(id: '1', name: 'test group', users: ['first user']) | true         | AuthorizationResult.approved()
        true      | new Revision(1L, 'client1', 'root') | new UserGroupDTO(id: '1', name: 'test group', users: ['first user']) | true         | AuthorizationResult.approved()
        false     | new Revision(0L, 'client1', 'root') | new UserGroupDTO(id: '1', name: 'test group', users: ['first user']) | false        | AuthorizationResult.denied()
        true      | new Revision(1L, 'client1', 'root') | new UserGroupDTO(id: '1', name: 'test group', users: ['first user']) | false        | AuthorizationResult.denied()
    }

    @Unroll
    def "DeleteUserGroup: isAuthorized: #isAuthorized, user exists: #userExists"() {
        given:
        def userGroupDao = Mock UserGroupDAO
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
        def userGroup = new Group.Builder().identifier(userGroupDto.id).name(userGroupDto.name).addUsers(userGroupDto.users).build()

        when:
        def userEntity = niFiServiceFacade.deleteUserGroup(currentRevision, userGroupDto.id)

        then:
        if (userExists) {
            1 * userGroupDao.getUserGroup(userGroupDto.id) >> userGroup
            1 * userGroupDao.deleteUserGroup(userGroupDto.id) >> userGroup
        } else {
            1 * userGroupDao.getUserGroup(userGroupDto.id) >> null
            1 * userGroupDao.deleteUserGroup(userGroupDto.id) >> null
        }
        1 * controllerFacade.save()
        1 * revisionManager.deleteRevision(_, _, _) >> { RevisionClaim revisionClaim, NiFiUser nifiUser, DeleteRevisionTask task ->
            task.performTask()
        }
        1 * authorizableLookup.getUserGroupsAuthorizable() >> new SimpleAuthorizable(null, ResourceFactory.getPolicyResource(userGroupDto.id),
                isAuthorized, authorizationResult)
        0 * _
        userEntity?.component?.id == null
        if (userExists) {
            assert userEntity?.id?.equals(userGroupDto.id)
        } else {
            assert userEntity?.id == null
        }

        where:
        userExists | currentRevision                     | userGroupDto                                                         | isAuthorized | authorizationResult
        true       | new Revision(1L, 'client1', 'root') | new UserGroupDTO(id: '1', name: 'test group', users: ['first user']) | true         | AuthorizationResult.approved()
        false      | null                                | new UserGroupDTO(id: '1', name: 'test group', users: ['first user']) | true         | AuthorizationResult.approved()
        true       | new Revision(1L, 'client1', 'root') | new UserGroupDTO(id: '1', name: 'test group', users: ['first user']) | false        | AuthorizationResult.denied()
        false      | null                                | new UserGroupDTO(id: '1', name: 'test group', users: ['first user']) | false        | AuthorizationResult.denied()
    }

    @Unroll
    def "CreateAccessPolicy: #isAuthorized"() {
        given:
        def accessPolicyDao = Mock AccessPolicyDAO
        def entityFactory = new EntityFactory()
        def dtoFactory = new DtoFactory()
        dtoFactory.setEntityFactory entityFactory
        def authorizableLookup = Mock AuthorizableLookup
        def niFiServiceFacade = new StandardNiFiServiceFacade()
        niFiServiceFacade.setAuthorizableLookup authorizableLookup
        niFiServiceFacade.setDtoFactory dtoFactory
        niFiServiceFacade.setAccessPolicyDAO accessPolicyDao
        niFiServiceFacade.setEntityFactory entityFactory
        def builder = new AccessPolicy.Builder().identifier(accessPolicyDto.id).resource(accessPolicyDto.resource).addUsers(accessPolicyDto.users).addGroups(accessPolicyDto.groups)
        if (accessPolicyDto.canRead) {
            builder.addAction(RequestAction.READ)
        }
        if (accessPolicyDto.canWrite) {
            builder.addAction(RequestAction.WRITE)
        }
        def newAccessPolicy = builder.build()

        when:
        def accessPolicyEntity = niFiServiceFacade.createAccessPolicy(new Revision(0L, 'client-1'), accessPolicyDto)

        then:
        1 * accessPolicyDao.createAccessPolicy(accessPolicyDto) >> newAccessPolicy
        1 * authorizableLookup.getAccessPolicyAuthorizable(accessPolicyDto.id) >> new SimpleAuthorizable(null, ResourceFactory.getPolicyResource(accessPolicyDto.id),
                isAuthorized, authorizationResult)
        0 * _
        accessPolicyEntity != null
        if (isAuthorized) {
            assert accessPolicyEntity?.component?.id?.equals(accessPolicyDto.id)
            assert accessPolicyEntity?.accessPolicy?.canRead
            assert accessPolicyEntity?.accessPolicy?.canWrite
        } else {
            assert accessPolicyEntity.component == null
        }

        where:
        accessPolicyDto                                                                                                 | isAuthorized | authorizationResult
        new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: ['user'], canRead: true) | true         | AuthorizationResult.approved()
        new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: ['user'], canRead: true) | false        | AuthorizationResult.denied()
    }

    @Unroll
    def "GetAccessPolicy: isAuthorized: #isAuthorized"() {
        given:
        def accessPolicyDao = Mock AccessPolicyDAO
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
        def builder = new AccessPolicy.Builder()
        builder.identifier(accessPolicyDto.id).resource(accessPolicyDto.resource).addUsers(accessPolicyDto.users).addGroups(accessPolicyDto.groups)
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
        if (isAuthorized) {
            1 * accessPolicyDao.getAccessPolicy(accessPolicyDto.id) >> requestedAccessPolicy
        }
        1 * revisionManager.get(_, _) >> { String id, ReadOnlyRevisionCallback callback ->
            callback.withRevision(new Revision(1L, 'client1', 'root'))
        }
        1 * authorizableLookup.getAccessPolicyAuthorizable(accessPolicyDto.id) >> new SimpleAuthorizable(null, ResourceFactory.getPolicyResource(accessPolicyDto.id),
                isAuthorized, authorizationResult)
        0 * _
        if (isAuthorized) {
            assert accessPolicyEntity?.component?.id?.equals(accessPolicyDto.id)
        } else {
            assert exception instanceof AccessDeniedException
        }

        where:
        accessPolicyDto                                                                                                 | isAuthorized | authorizationResult
        new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: ['user'], canRead: true) | true         | AuthorizationResult.approved()
        new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: ['user'], canRead: true) | false        | AuthorizationResult.denied()
    }

    @Unroll
    def "UpdateAccessPolicy: isAuthorized: #isAuthorized, policy exists: #hasPolicy"() {
        given:
        def accessPolicyDao = Mock AccessPolicyDAO
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
        def builder = new AccessPolicy.Builder().identifier(accessPolicyDto.id).resource(accessPolicyDto.resource).addUsers(accessPolicyDto.users).addGroups(accessPolicyDto.groups)
        if (accessPolicyDto.canRead) {
            builder.addAction(RequestAction.READ)
        }
        if (accessPolicyDto.canWrite) {
            builder.addAction(RequestAction.WRITE)
        }
        def accessPolicy = builder.build()

        when:
        def accessPolicyEntityUpdateResult = niFiServiceFacade.updateAccessPolicy(currentRevision, accessPolicyDto)

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
        }
        1 * authorizableLookup.getAccessPolicyAuthorizable(accessPolicyDto.id) >> new SimpleAuthorizable(null, ResourceFactory.getPolicyResource(accessPolicyDto.id),
                isAuthorized, authorizationResult)
        0 * _
        accessPolicyEntityUpdateResult != null
        def accessPolicyEntity = accessPolicyEntityUpdateResult?.result
        if (isAuthorized) {
            assert accessPolicyEntity?.component?.id?.equals(accessPolicyDto.id)
            assert accessPolicyEntity?.accessPolicy?.canRead
            assert accessPolicyEntity?.accessPolicy?.canWrite
        } else {
            assert accessPolicyEntity.component == null
        }

        where:
        hasPolicy | currentRevision                     | accessPolicyDto                                                                                                 | isAuthorized |
                authorizationResult
        false     | new Revision(0L, 'client1', 'root') | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: ['user'], canRead: true) | true         |
                AuthorizationResult.approved()
        true      | new Revision(1L, 'client1', 'root') | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: ['user'], canRead: true) | true         |
                AuthorizationResult.approved()
        false     | new Revision(0L, 'client1', 'root') | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: ['user'], canRead: true) | false        |
                AuthorizationResult.denied()
        true      | new Revision(1L, 'client1', 'root') | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: ['user'], canRead: true) | false        |
                AuthorizationResult.denied()
    }

    @Unroll
    def "DeleteAccessPolicy: isAuthorized: #isAuthorized, user exists: #userExists"() {
        given:
        def accessPolicyDao = Mock AccessPolicyDAO
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
        def builder = new AccessPolicy.Builder()
        builder.identifier(accessPolicyDto.id).resource(accessPolicyDto.resource).addUsers(accessPolicyDto.users).addGroups(accessPolicyDto.groups)
        if (accessPolicyDto.canRead) {
            builder.addAction(RequestAction.READ)
        }
        if (accessPolicyDto.canWrite) {
            builder.addAction(RequestAction.WRITE)
        }
        def accessPolicy = builder.build()

        when:
        def accessPolicyEntity = niFiServiceFacade.deleteAccessPolicy(currentRevision, accessPolicyDto.id)

        then:
        if (userExists) {
            1 * accessPolicyDao.getAccessPolicy(accessPolicyDto.id) >> accessPolicy
            1 * accessPolicyDao.deleteAccessPolicy(accessPolicyDto.id) >> accessPolicy
        } else {
            1 * accessPolicyDao.getAccessPolicy(accessPolicyDto.id) >> null
            1 * accessPolicyDao.deleteAccessPolicy(accessPolicyDto.id) >> null
        }
        1 * controllerFacade.save()
        1 * revisionManager.deleteRevision(_, _, _) >> { RevisionClaim revisionClaim, NiFiUser nifiUser, DeleteRevisionTask task ->
            task.performTask()
        }
        1 * authorizableLookup.getAccessPolicyAuthorizable(accessPolicyDto.id) >> new SimpleAuthorizable(null, ResourceFactory.getPolicyResource(accessPolicyDto.id),
                isAuthorized, authorizationResult)
        0 * _
        accessPolicyEntity?.component?.id == null
        if (userExists) {
            assert accessPolicyEntity?.id?.equals(accessPolicyDto.id)
        } else {
            assert accessPolicyEntity?.id == null
        }

        where:
        userExists | currentRevision                     | accessPolicyDto                                                                                                 | isAuthorized |
                authorizationResult
        true       | new Revision(1L, 'client1', 'root') | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: ['user'], canRead: true) | true         |
                AuthorizationResult.approved()
        false      | null                                | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: ['user'], canRead: true) | true         |
                AuthorizationResult.approved()
        true       | new Revision(1L, 'client1', 'root') | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: ['user'], canRead: true) | false        |
                AuthorizationResult.denied()
        false      | null                                | new AccessPolicyDTO(id: '1', resource: ResourceFactory.flowResource.identifier, users: ['user'], canRead: true) | false        |
                AuthorizationResult.denied()
    }

    private class SimpleAuthorizable implements Authorizable {
        final private Authorizable parentAuthorizable;
        final private Resource resource;
        final private boolean isAuthorized;
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
        boolean isAuthorized(Authorizer authorzr, RequestAction action) {
            return isAuthorized
        }

        @Override
        AuthorizationResult checkAuthorization(Authorizer authorzr, RequestAction action) {
            return authorizationResult
        }

        @Override
        void authorize(Authorizer authorzr, RequestAction action) throws AccessDeniedException {
            if (!isAuthorized) {
                throw new AccessDeniedException("test exception, access denied")
            }
        }
    }
}