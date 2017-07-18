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
package org.apache.nifi.web.dao.impl

import org.apache.nifi.authorization.*
import org.apache.nifi.web.ResourceNotFoundException
import org.apache.nifi.web.api.dto.AccessPolicyDTO
import org.apache.nifi.web.api.dto.UserDTO
import org.apache.nifi.web.api.dto.UserGroupDTO
import org.apache.nifi.web.api.entity.TenantEntity
import spock.lang.Specification
import spock.lang.Unroll

class StandardPolicyBasedAuthorizerDAOSpec extends Specification {

    private AbstractPolicyBasedAuthorizer mockAuthorizer() {
        def authorizer = Mock AbstractPolicyBasedAuthorizer
        authorizer.getAccessPolicyProvider() >> {
            callRealMethod();
        }
        return authorizer;
    }

    @Unroll
    def "test non-policy-based authorizer #method throws IllegalStateException"() {
        when:
        daoMethod()

        then:
        def e = thrown(IllegalStateException)
        assert e.message.equalsIgnoreCase(StandardPolicyBasedAuthorizerDAO.MSG_NON_MANAGED_AUTHORIZER)

        where:
        method               | daoMethod
        'getAccessPolicy'    | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).getAccessPolicy('1') }
        'getUser'            | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).getUser('1') }
        'getUserGroup'       | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).getUserGroup('1') }
        'hasAccessPolicy'    | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).hasAccessPolicy('1') }
        'hasUser'            | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).hasUser('1') }
        'hasUserGroup'       | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).hasUserGroup('1') }
    }

    @Unroll
    def "test non-configurable user group provider #method throws IllegalStateException"() {
        when:
        daoMethod()

        then:
        def e = thrown(IllegalStateException)
        assert e.message.equalsIgnoreCase(StandardPolicyBasedAuthorizerDAO.MSG_NON_CONFIGURABLE_USERS)

        where:
        method               | daoMethod
        'createUser'         | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).createUser(new UserDTO(id: '1', identity: 'a')) }
        'createUserGroup'    | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).createUserGroup(new UserGroupDTO(id: '1', identity: 'a')) }
        'deleteUser'         | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).deleteUser('1') }
        'deleteUserGroup'    | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).deleteUserGroup('1') }
        'updateUser'         | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).updateUser(new UserDTO(id: '1', identity: 'a')) }
        'updateUserGroup'    | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).updateUserGroup(new UserGroupDTO(id: '1', identity: 'a')) }
    }

    @Unroll
    def "test non-configurable access policy provider #method throws IllegalStateException"() {
        when:
        daoMethod()

        then:
        def e = thrown(IllegalStateException)
        assert e.message.equalsIgnoreCase(StandardPolicyBasedAuthorizerDAO.MSG_NON_CONFIGURABLE_POLICIES)

        where:
        method               | daoMethod
        'createAccessPolicy' | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).createAccessPolicy(new AccessPolicyDTO(id: '1', resource: '/1', action: "read")) }
        'deleteAccessPolicy' | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).deleteAccessPolicy('1') }
        'updateAccessPolicy' | { new StandardPolicyBasedAuthorizerDAO(Mock(Authorizer)).updateAccessPolicy(new AccessPolicyDTO(id: '1', resource: '/1', action: "read")) }
    }

    @Unroll
    def "HasAccessPolicy: accessPolicy: #accessPolicy"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        def result = dao.hasAccessPolicy('policy-id-1')

        then:
        1 * authorizer.getAccessPolicy('policy-id-1') >> accessPolicy
        0 * _
        result == (accessPolicy != null)

        where:
        accessPolicy                                                                  | _
        new AccessPolicy.Builder().identifier('policy-id-1').resource('/fake/resource').addUser('user-id-1').addGroup('user-group-id-1')
                .action(RequestAction.WRITE).build() | _
        null                                                                          | _
    }

    @Unroll
    def "CreateAccessPolicy: accessPolicy=#accessPolicy"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)
        def requestDTO = new AccessPolicyDTO(id: 'policy-id-1', resource: '/fake/resource', action: "read",
                users: [new TenantEntity(id: 'user-id-1')] as Set,
                userGroups: [new TenantEntity(id: 'user-group-id-1')] as Set)

        when:
        def result = dao.createAccessPolicy(requestDTO)

        then:
        noExceptionThrown()

        then:
        1 * authorizer.doAddAccessPolicy(accessPolicy) >> accessPolicy
        0 * _
        result?.equals accessPolicy

        where:
        accessPolicy                                 | accessPolicies
        new AccessPolicy.Builder().identifier('policy-id-1').resource('/fake/resource').addUser('user-id-1').addGroup('user-group-id-1')
                .action(RequestAction.WRITE).build() | [] as Set
    }

    @Unroll
    def "GetAccessPolicy: success"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        def result = dao.getAccessPolicy('policy-id-1')

        then:
        1 * authorizer.getAccessPolicy('policy-id-1') >> accessPolicy
        0 * _
        assert result?.equals(accessPolicy)

        where:
        accessPolicy                                                                  | _
        new AccessPolicy.Builder().identifier('policy-id-1').resource('/fake/resource').addUser('user-id-1').addGroup('user-group-id-1')
                .action(RequestAction.WRITE).build() | _
    }

    @Unroll
    def "GetAccessPolicy: failure"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        dao.getAccessPolicy('policy-id-1')

        then:
        1 * authorizer.getAccessPolicy('policy-id-1') >> null
        0 * _
        thrown ResourceNotFoundException
    }

    @Unroll
    def "UpdateAccessPolicy: success"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)
        def requestDTO = new AccessPolicyDTO(id: 'policy-id-1', resource: '/fake/resource', action: "read",
                users: [new TenantEntity(id: 'user-id-1')] as Set,
                userGroups: [new TenantEntity(id: 'user-group-id-1')] as Set)

        when:
        def result = dao.updateAccessPolicy(requestDTO)

        then:
        1 * authorizer.getAccessPolicy(requestDTO.id) >> accessPolicy
        1 * authorizer.updateAccessPolicy(accessPolicy) >> accessPolicy
        0 * _
        result?.equals(accessPolicy)

        where:
        accessPolicy                                                                  | _
        new AccessPolicy.Builder().identifier('policy-id-1').resource('/fake/resource').addUser('user-id-1').addGroup('user-group-id-1')
                .action(RequestAction.WRITE).build() | _
    }

    @Unroll
    def "UpdateAccessPolicy: failure"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)
        def requestDTO = new AccessPolicyDTO(id: 'policy-id-1', resource: '/fake/resource', action: "read",
                users: [new TenantEntity(id: 'user-id-1')] as Set,
                userGroups: [new TenantEntity(id: 'user-group-id-1')] as Set)

        when:
        dao.updateAccessPolicy(requestDTO)

        then:
        1 * authorizer.getAccessPolicy(requestDTO.id) >> null
        0 * _
        thrown ResourceNotFoundException
    }

    @Unroll
    def "DeleteAccessPolicy: success"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        def result = dao.deleteAccessPolicy('policy-id-1')

        then:
        1 * authorizer.getAccessPolicy('policy-id-1') >> accessPolicy
        1 * authorizer.deleteAccessPolicy(accessPolicy) >> accessPolicy
        0 * _
        result?.equals(accessPolicy)

        where:
        accessPolicy                                                                  | _
        new AccessPolicy.Builder().identifier('policy-id-1').resource('/fake/resource').addUser('user-id-1').addGroup('user-group-id-1')
                .action(RequestAction.WRITE).build() | _
    }

    @Unroll
    def "DeleteAccessPolicy: failure"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        dao.deleteAccessPolicy('policy-id-1')

        then:
        1 * authorizer.getAccessPolicy('policy-id-1') >> null
        0 * _
        thrown ResourceNotFoundException
    }

    @Unroll
    def "HasUserGroup: userGroup=#userGroup"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        def result = dao.hasUserGroup('user-group-id-1')

        then:
        1 * authorizer.getGroup('user-group-id-1') >> userGroup
        0 * _
        result == (userGroup != null)

        where:
        userGroup                                                                                              | _
        new Group.Builder().identifier('user-group-id-1').name('user-group-id-1').addUser('user-id-1').build() | _
        null                                                                                                   | _
    }

    @Unroll
    def "CreateUserGroup: userGroup=#userGroup"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)
        def requestDTO = new UserGroupDTO(id: 'user-group-id-1', identity: 'user group identity', users: [new TenantEntity(id: 'user-id-1')] as Set)

        when:
        def result = dao.createUserGroup(requestDTO)

        then:
        noExceptionThrown()

        then:
        1 * authorizer.doAddGroup(userGroup) >> userGroup
        0 * _
        result?.equals userGroup

        where:
        userGroup                                                       | users         | groups
        new Group.Builder().identifier('user-group-id-1')
                .name('user-group-id-1').addUser('user-id-1').build()   | [] as Set     | [] as Set
    }

    @Unroll
    def "GetUserGroup: success"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        def result = dao.getUserGroup('user-group-id-1')

        then:
        1 * authorizer.getGroup('user-group-id-1') >> userGroup
        0 * _
        result?.equals(userGroup)

        where:
        userGroup                                                                                              | _
        new Group.Builder().identifier('user-group-id-1').name('user-group-id-1').addUser('user-id-1').build() | _
    }

    @Unroll
    def "GetUserGroup: failure"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        dao.getUserGroup('user-group-id-1')

        then:
        1 * authorizer.getGroup('user-group-id-1') >> null
        0 * _
        thrown ResourceNotFoundException
    }

    @Unroll
    def "GetUserGroups: success"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        def result = dao.getUserGroups()

        then:
        1 * authorizer.getGroups() >> userGroups
        0 * _
        result?.equals(userGroups)

        where:
        userGroups                                                                                             | _
        [new Group.Builder().identifier('user-group-id-1').name('user-group-id-1').addUser('user-id-1').build()] as Set | _
    }

    @Unroll
    def "UpdateUserGroup: success"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)
        def requestDTO = new UserGroupDTO(id: 'user-group-id-1', identity: 'user group identity', users: [new TenantEntity(id: 'user-id-1')] as Set)

        when:
        def result = dao.updateUserGroup(requestDTO)

        then:
        1 * authorizer.getGroup(requestDTO.id) >> userGroup
        1 * authorizer.doUpdateGroup(userGroup) >> userGroup
        0 * _
        result?.equals(userGroup)

        where:
        userGroup                                                     | users       | groups
        new Group.Builder().identifier('user-group-id-1')
                .name('user-group-id-1').addUser('user-id-1').build() | [] as Set   | [] as Set
    }

    @Unroll
    def "UpdateUserGroup: failure"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)
        def requestDTO = new UserGroupDTO(id: 'user-group-id-1', identity: 'user group identity', users: [new TenantEntity(id: 'user-id-1')] as Set)

        when:
        dao.updateUserGroup(requestDTO)

        then:
        1 * authorizer.getGroup(requestDTO.id) >> null
        0 * _
        thrown ResourceNotFoundException
    }

    @Unroll
    def "DeleteUserGroup: success"() {
        given:
        def authorizer = mockAuthorizer()
        authorizer.getAccessPolicyProvider().getAccessPolicies() >> {
            callRealMethod();
        }
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        def result = dao.deleteUserGroup('user-group-id-1')

        then:
        1 * authorizer.getGroup('user-group-id-1') >> userGroup
        1 * authorizer.deleteGroup(userGroup) >> userGroup
        1 * authorizer.getAccessPolicies() >> []
        0 * _
        assert result?.equals(userGroup)

        where:
        userGroup                                                                                              | _
        new Group.Builder().identifier('user-group-id-1').name('user-group-id-1').addUser('user-id-1').build() | _
    }

    @Unroll
    def "DeleteUserGroup: failure"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        dao.deleteUserGroup('user-group-id-1')

        then:
        1 * authorizer.getGroup('user-group-id-1') >> null
        0 * _
        thrown ResourceNotFoundException
    }

    @Unroll
    def "HasUser: user=#user"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        def result = dao.hasUser('user-id-1')

        then:
        1 * authorizer.getUser('user-id-1') >> user
        0 * _
        result == (user != null)

        where:
        user                                                                                                     | _
        new User.Builder().identifier('user-id-1').identity('user identity').build() | _
    }

    @Unroll
    def "CreateUser: user=#user"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)
        def requestDTO = new UserDTO(id: 'user-id-1', identity: 'user identity', userGroups: [new TenantEntity(id: 'user-group-id-1')] as Set)

        when:
        def result = dao.createUser(requestDTO)

        then:
        noExceptionThrown()

        then:
        1 * authorizer.doAddUser(user) >> user
        0 * _
        result?.equals user

        where:
        user                                        | users         | groups
        new User.Builder().identifier('user-id-1')
                .identity('user identity').build()  | [] as Set     | [] as Set
    }

    @Unroll
    def "GetUser: success"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        def result = dao.getUser('user-id-1')

        then:
        1 * authorizer.getUser('user-id-1') >> user
        result?.equals(user)
        0 * _

        where:
        user                                                                                                     | _
        new User.Builder().identifier('user-id-1').identity('user identity').build() | _
    }

    @Unroll
    def "GetUser: failure"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        dao.getUser('user-id-1')

        then:
        1 * authorizer.getUser('user-id-1') >> null
        0 * _
        thrown ResourceNotFoundException
    }

    @Unroll
    def "GetUsers: success"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        def result = dao.getUsers()

        then:
        1 * authorizer.getUsers() >> users
        result?.containsAll(users)
        0 * _

        where:
        users                                                                                                             | _
        [new User.Builder().identifier('user-id-1').identity('user identity').build()] as Set | _
    }

    @Unroll
    def "UpdateUser: success"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)
        def requestDTO = new UserDTO(id: 'user-id-1', identity: 'user identity', userGroups: [new TenantEntity(id: 'user-group-id-1')] as Set)

        when:
        def result = dao.updateUser(requestDTO)

        then:
        1 * authorizer.getUser(requestDTO.id) >> user
        1 * authorizer.doUpdateUser(user) >> user
        0 * _
        result?.equals(user)

        where:
        user                                        | users         | groups
        new User.Builder().identifier('user-id-1')
                .identity('user identity').build()  | [] as Set     | [] as Set
    }

    @Unroll
    def "UpdateUser: failure"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)
        def requestDTO = new UserDTO(id: 'user-id-1', identity: 'user identity', userGroups: [new TenantEntity(id: 'user-group-id-1')] as Set)

        when:
        dao.updateUser(requestDTO)

        then:
        1 * authorizer.getUser(requestDTO.id) >> null
        0 * _
        thrown ResourceNotFoundException
    }

    @Unroll
    def "DeleteUser: success"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        def result = dao.deleteUser('user-id-1')

        then:
        1 * authorizer.getUser('user-id-1') >> user
        1 * authorizer.deleteUser(user) >> user
        1 * authorizer.getAccessPolicies() >> []
        0 * _
        result?.equals(user)

        where:
        user                                                                                                     | _
        new User.Builder().identifier('user-id-1').identity('user identity').build() | _
    }

    @Unroll
    def "DeleteUser: failure"() {
        given:
        def authorizer = mockAuthorizer()
        def dao = new StandardPolicyBasedAuthorizerDAO(authorizer)

        when:
        dao.deleteUser('user-id-1')

        then:
        1 * authorizer.getUser('user-id-1') >> null
        0 * _
        thrown ResourceNotFoundException
    }
}
