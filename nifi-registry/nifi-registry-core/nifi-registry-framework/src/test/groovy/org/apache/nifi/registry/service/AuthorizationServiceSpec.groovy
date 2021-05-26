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
package org.apache.nifi.registry.service

import org.apache.nifi.registry.authorization.AccessPolicy
import org.apache.nifi.registry.authorization.User
import org.apache.nifi.registry.authorization.UserGroup
import org.apache.nifi.registry.bucket.Bucket
import org.apache.nifi.registry.exception.ResourceNotFoundException
import org.apache.nifi.registry.security.authorization.*
import org.apache.nifi.registry.security.authorization.AccessPolicy as AuthAccessPolicy
import org.apache.nifi.registry.security.authorization.User as AuthUser
import org.apache.nifi.registry.security.authorization.exception.AccessDeniedException
import org.apache.nifi.registry.security.authorization.resource.Authorizable
import org.apache.nifi.registry.security.authorization.resource.ResourceType
import spock.lang.Specification

class AuthorizationServiceSpec extends Specification {

    def registryService = Mock(RegistryService)
    def authorizableLookup = Mock(AuthorizableLookup)
    def userGroupProvider = Mock(ConfigurableUserGroupProvider)
    def accessPolicyProvider = Mock(ConfigurableAccessPolicyProvider)

    AuthorizationService authorizationService

    def setup() {
        accessPolicyProvider.getUserGroupProvider() >> userGroupProvider
        def standardAuthorizer = new StandardManagedAuthorizer(accessPolicyProvider, userGroupProvider)
        authorizationService = new AuthorizationService(authorizableLookup, standardAuthorizer, registryService)
    }

    // ----- User tests -------------------------------------------------------

    def "create user"() {

        setup:
        userGroupProvider.addUser(!null as AuthUser) >> {
            AuthUser u -> new AuthUser.Builder().identifier(u.identifier).identity(u.identity).build()
        }
        userGroupProvider.getGroups() >> new HashSet<Group>()  // needed for converting user to DTO
        accessPolicyProvider.getAccessPolicies() >> new HashSet<AccessPolicy>()  // needed for converting user to DTO

        when: "new user is created successfully"
        def user = new User("id", "username")
        User createdUser = authorizationService.createUser(user)

        then: "created user has been assigned an identifier"
        with(createdUser) {
            identifier == "id"
            identity == "username"
        }

    }

    def "list users"() {

        setup:
        userGroupProvider.getUsers() >> [
                new AuthUser.Builder().identifier("user1").identity("username1").build(),
                new AuthUser.Builder().identifier("user2").identity("username2").build(),
                new AuthUser.Builder().identifier("user3").identity("username3").build(),
        ]
        userGroupProvider.getGroups() >> new HashSet<Group>()
        accessPolicyProvider.getAccessPolicies() >> new HashSet<AccessPolicy>()

        when: "list of users is queried"
        def users = authorizationService.getUsers()

        then: "users are successfully returned as list of DTO objects"
        users != null
        users.size() == 3
        with(users[0]) {
            identifier == "user1"
            identity == "username1"
        }
        with(users[1]) {
            identifier == "user2"
            identity == "username2"
        }
        with(users[2]) {
            identifier == "user3"
            identity == "username3"
        }

    }

    def "get user"() {

        setup:
        def user1 = new AuthUser.Builder().identifier("user-id-1").identity("user1").build()
        def group1 = new Group.Builder().identifier("group-id-1").name("group1").addUser("user-id-1").build()
        def apBuilder = new org.apache.nifi.registry.security.authorization.AccessPolicy.Builder().resource("/fake-resource").action(RequestAction.READ)
        def ap1 = apBuilder.identifier("policy-1").addUser("user-id-1").build()
        def ap2 = apBuilder.identifier("policy-2").clearUsers().addGroup("group-id-1").build()
        def ap3 = apBuilder.identifier("policy-3").clearGroups().addGroup("does-not-exist").build()
        userGroupProvider.getUser("does-not-exist") >> null
        userGroupProvider.getUser("user-id-1") >> user1
        userGroupProvider.getGroup("group-id-1") >> group1
        userGroupProvider.getGroup("does-not-exist") >> null
        userGroupProvider.getGroups() >> new HashSet<Group>([group1])
        accessPolicyProvider.getAccessPolicies() >> new HashSet<>([ap1, ap2, ap3])


        when: "get user for existing user identifier"
        def userDto1 = authorizationService.getUser("user-id-1")

        then: "user is returned converted to DTO"
        with(userDto1) {
            identifier == "user-id-1"
            identity == "user1"
            userGroups.size() == 1
            userGroups[0].identifier == "group-id-1"
            accessPolicies.size() == 2
            accessPolicies.stream().noneMatch({it.identifier == "policy-3"})
        }


        when: "get user for non-existent tenant identifier"
        def user2 = authorizationService.getUser("does-not-exist")

        then: "no user is returned"
        thrown(ResourceNotFoundException.class)

    }

    def "update user"() {

        setup:
        userGroupProvider.updateUser(!null as AuthUser) >> {
            AuthUser u -> new AuthUser.Builder().identifier(u.identifier).identity(u.identity).build()
        }
        userGroupProvider.getGroups() >> new HashSet<Group>()
        accessPolicyProvider.getAccessPolicies() >> new HashSet<AccessPolicy>()


        when: "user is updated"
        def user = authorizationService.updateUser(new User("userId", "username"))

        then: "updated user is returned"
        with(user) {
            identifier == "userId"
            identity == "username"
        }

    }

    def "delete user"() {

        setup:
        def user1 = new AuthUser.Builder().identifier("userId").identity("username").build()
        userGroupProvider.getUser("userId") >> user1
        userGroupProvider.deleteUser(user1) >> user1
        userGroupProvider.getGroups() >> new HashSet<Group>()
        accessPolicyProvider.getAccessPolicies() >> new HashSet<AccessPolicy>()


        when: "user is deleted"
        def user = authorizationService.deleteUser("userId")

        then: "deleted user is returned converted to DTO"
        with(user) {
            identifier == "userId"
            identity == "username"
        }

    }

    // ----- User Group tests -------------------------------------------------

    def "create user group"() {

        setup:
        userGroupProvider.addGroup(!null as Group) >> {
            Group g -> new Group.Builder().identifier(g.identifier).name(g.name).build()
        }
        accessPolicyProvider.getAccessPolicies() >> new HashSet<AccessPolicy>()  // needed for converting to DTO

        when: "new group is created successfully"
        def group = new UserGroup("id", "groupName")
        UserGroup createdGroup = authorizationService.createUserGroup(group)

        then: "created group has been assigned an identifier"
        with(createdGroup) {
            identifier == "id"
            identity == "groupName"
        }

    }

    def "list user groups"() {

        setup:
        userGroupProvider.getGroups() >> [
                new Group.Builder().identifier("groupId1").name("groupName1").build(),
                new Group.Builder().identifier("groupId2").name("groupName2").build(),
                new Group.Builder().identifier("groupId3").name("groupName3").build(),
        ]
        accessPolicyProvider.getAccessPolicies() >> new HashSet<AccessPolicy>()

        when: "list of groups is queried"
        def groups = authorizationService.getUserGroups()

        then: "groups are successfully returned as list of DTO objects"
        groups != null
        groups.size() == 3
        with(groups[0]) {
            identifier == "groupId1"
            identity == "groupName1"
        }
        with(groups[1]) {
            identifier == "groupId2"
            identity == "groupName2"
        }
        with(groups[2]) {
            identifier == "groupId3"
            identity == "groupName3"
        }

    }

    def "get user group"() {

        setup:
        accessPolicyProvider.getAccessPolicies() >> new HashSet<AccessPolicy>()


        when: "get group for existing user identifier"
        userGroupProvider.getGroup("groupId") >> new Group.Builder().identifier("groupId").name ("groupName").build()
        def g1 = authorizationService.getUserGroup("groupId")

        then: "group is returned converted to DTO"
        with(g1) {
            identifier == "groupId"
            identity == "groupName"
        }


        when: "get group for non-existent group identifier"
        userGroupProvider.getUser("nonExistentId") >> null
        userGroupProvider.getGroup("nonExistentId") >> null
        def g2 = authorizationService.getUserGroup("nonExistentId")

        then: "no group is returned"
        thrown(ResourceNotFoundException.class)

    }

    def "update user group"() {

        setup:
        userGroupProvider.updateGroup(!null as Group) >> {
            Group g -> new Group.Builder().identifier(g.identifier).name(g.name).build()
        }
        accessPolicyProvider.getAccessPolicies() >> new HashSet<AccessPolicy>()


        when: "group is updated"
        def group = authorizationService.updateUserGroup(new UserGroup("id", "name"))

        then: "updated group is returned converted to DTO"
        with(group) {
            identifier == "id"
            identity == "name"
        }

    }

    def "delete user group"() {

        setup:
        def group1 = new Group.Builder().identifier("id").name("name").build();
        userGroupProvider.getGroup("id") >> group1
        userGroupProvider.deleteGroup(group1) >> group1
        accessPolicyProvider.getAccessPolicies() >> new HashSet<AccessPolicy>()


        when: "group is deleted"
        def group = authorizationService.deleteUserGroup("id")

        then: "deleted user is returned"
        with(group) {
            identifier == "id"
            identity == "name"
        }

    }

    // ----- Access Policy tests ----------------------------------------------

    def "create access policy"() {

        setup:
        accessPolicyProvider.addAccessPolicy(!null as AuthAccessPolicy) >> {
            AuthAccessPolicy p -> new AuthAccessPolicy.Builder()
                    .identifier(p.identifier)
                    .resource(p.resource)
                    .action(p.action)
                    .addGroups(p.groups)
                    .addUsers(p.users)
                    .build()
        }
        accessPolicyProvider.isConfigurable(_ as AuthAccessPolicy) >> true


        when: "new access policy is created successfully"
        def accessPolicy = new AccessPolicy([resource: "/resource", action: "read"])
        accessPolicy.setIdentifier("id")

        def createdPolicy = authorizationService.createAccessPolicy(accessPolicy)

        then: "created policy has been assigned an identifier"
        with(createdPolicy) {
            identifier == "id"
            resource == "/resource"
            action == "read"
            configurable == true
        }

    }

    def "list access policies"() {

        setup:
        accessPolicyProvider.getAccessPolicies() >> [
                new AuthAccessPolicy.Builder().identifier("ap1").resource("r1").action(RequestAction.READ).build(),
                new AuthAccessPolicy.Builder().identifier("ap2").resource("r2").action(RequestAction.WRITE).build()
        ]

        when: "list access polices is queried"
        def policies = authorizationService.getAccessPolicies()

        then: "access policies are successfully returned as list of DTO objects"
        policies != null
        policies.size() == 2
        with(policies[0]) {
            identifier == "ap1"
            resource == "r1"
            action == RequestAction.READ.toString()
        }
        with(policies[1]) {
            identifier == "ap2"
            resource == "r2"
            action == RequestAction.WRITE.toString()
        }

    }

    def "get access policy"() {

        when: "get policy for existing identifier"
        accessPolicyProvider.getAccessPolicy("id") >> new AuthAccessPolicy.Builder()
                .identifier("id")
                .resource("/resource")
                .action(RequestAction.READ)
                .build()
        def p1 = authorizationService.getAccessPolicy("id")

        then: "policy is returned converted to DTO"
        with(p1) {
            identifier == "id"
            resource == "/resource"
            action == RequestAction.READ.toString()
        }


        when: "get policy for non-existent identifier"
        accessPolicyProvider.getAccessPolicy("nonExistentId") >> null
        def p2 = authorizationService.getAccessPolicy("nonExistentId")

        then: "no policy is returned"
        thrown(ResourceNotFoundException.class)

    }

    def "update access policy"() {

        setup:
        def users = [
                "user1": "alice",
                "user2": "bob",
                "user3": "charlie" ]
        def groups = [
                "group1": "users",
                "group2": "devs",
                "group3": "admins" ]
        def policies = [
                "policy1": [
                        "resource": "/resource1",
                        "action": "read",
                        "users": [ "user1" ],
                        "groups": []
                ]
        ]
        def mapDtoUser = { String id -> new User(id, users[id])}
        def mapDtoGroup = { String id -> new UserGroup(id, groups[id])}
        def mapAuthUser = { String id -> new AuthUser.Builder().identifier(id).identity(users[id]).build() }
        def mapAuthGroup = { String id -> new Group.Builder().identifier(id).name(groups[id]).build() }
        def mapAuthAccessPolicy = {
            String id -> return new AuthAccessPolicy.Builder()
                    .identifier(id)
                    .resource(policies[id]["resource"] as String)
                    .action(RequestAction.valueOfValue(policies[id]["action"] as String))
                    .addUsers(policies[id]["users"] as Set<String>)
                    .addGroups(policies[id]["groups"] as Set<String>)
                    .build()
        }
        userGroupProvider.getUser(!null as String) >> { String id -> users.containsKey(id) ? mapAuthUser(id) : null }
        userGroupProvider.getGroup(!null as String) >> { String id -> groups.containsKey(id) ? mapAuthGroup(id) : null }
        userGroupProvider.getUsers() >> {
            def authUsers = []
            users.each{ k, v -> authUsers.add(new AuthUser.Builder().identifier(k).identity(v).build()) }
            return authUsers
        }
        userGroupProvider.getGroups() >> {
            def authGroups = []
            users.each{ k, v -> authGroups.add(new Group.Builder().identifier(k).name(v).build()) }
            return authGroups
        }
        accessPolicyProvider.getAccessPolicy(!null as String) >> { String id -> policies.containsKey(id) ? mapAuthAccessPolicy(id) : null }
        accessPolicyProvider.updateAccessPolicy(!null as AuthAccessPolicy) >> {
            AuthAccessPolicy p -> new AuthAccessPolicy.Builder()
                    .identifier(p.identifier)
                    .resource(p.resource)
                    .action(p.action)
                    .addGroups(p.groups)
                    .addUsers(p.users)
                    .build()
        }
        accessPolicyProvider.isConfigurable(_ as AuthAccessPolicy) >> true


        when: "policy is updated"
        def policy = new AccessPolicy([identifier: "policy1", resource: "/resource1", action: "read"])
        policy.addUsers([mapDtoUser("user1"), mapDtoUser("user2")])
        policy.addUserGroups([mapDtoGroup("group1")])
        def p1 = authorizationService.updateAccessPolicy(policy)

        then: "updated group is returned converted to DTO"
        p1 != null
        p1.users.size() == 2
        def sortedUsers = p1.users.sort{it.identifier}
        with(sortedUsers[0]) {
            identifier == "user1"
            identity == "alice"
        }
        with(sortedUsers[1]) {
            identifier == "user2"
            identity == "bob"
        }
        p1.userGroups.size() == 1
        with(p1.userGroups[0]) {
            identifier == "group1"
            identity == "users"
        }


        when: "attempt to change policy resource and action"
        def p2 = authorizationService.updateAccessPolicy(new AccessPolicy([identifier: "policy1", resource: "/newResource", action: "write"]))

        then: "resource and action are unchanged"
        with(p2) {
            identifier == "policy1"
            resource == "/resource1"
            action == "read"
        }

    }

    def "delete access policy"() {

        setup:
        def policy1 = new AuthAccessPolicy.Builder()
                .identifier("policy1")
                .resource("/resource")
                .action(RequestAction.READ)
                .addGroups(new HashSet<String>())
                .addUsers(new HashSet<String>())
                .build()

        userGroupProvider.getGroups() >> new HashSet<Group>()
        userGroupProvider.getUsers() >> new HashSet<AuthUser>()
        accessPolicyProvider.getAccessPolicy("id") >> policy1
        accessPolicyProvider.deleteAccessPolicy(!null as String) >> policy1

        when: "access policy is deleted"
        def policy = authorizationService.deleteAccessPolicy("id")

        then: "deleted policy is returned"
        with(policy) {
            identifier == "policy1"
            resource == "/resource"
            action == RequestAction.READ.toString()
        }

    }

    // ----- Resource tests ---------------------------------------------------

    def "get resources"() {

        setup:
        def buckets = [
                "b1": [
                        "name": "Bucket #1",
                        "description": "An initial bucket for testing",
                        "createdTimestamp": 1
                ],
                "b2": [
                        "name": "Bucket #2",
                        "description": "A second bucket for testing",
                        "createdTimestamp": 2
                ],
        ]
        def mapBucket = {
            String id -> new Bucket([
                    identifier: id,
                    name: buckets[id]["name"] as String,
                    description: buckets[id]["description"] as String]) }

        registryService.getBuckets() >> {[ mapBucket("b1"), mapBucket("b2") ]}

        when:
        def resources = authorizationService.getResources()

        then:
        resources != null
        resources.size() == 8
        def sortedResources = resources.sort{it.identifier}
        sortedResources[0].identifier == "/actuator"
        sortedResources[1].identifier == "/buckets"
        sortedResources[2].identifier == "/buckets/b1"
        sortedResources[3].identifier == "/buckets/b2"
        sortedResources[4].identifier == "/policies"
        sortedResources[5].identifier == "/proxy"
        sortedResources[6].identifier == "/swagger"
        sortedResources[7].identifier == "/tenants"

    }

    def "get authorized resources"() {

        setup:
        def buckets = [
                "b1": [
                        "name": "Bucket #1",
                        "description": "An initial bucket for testing",
                        "createdTimestamp": 1,
                        "allowPublicRead" : false
                ],
                "b2": [
                        "name": "Bucket #2",
                        "description": "A second bucket for testing",
                        "createdTimestamp": 2,
                        "allowPublicRead" : true
                ],
                "b3": [
                        "name": "Bucket #3",
                        "description": "A third bucket for testing",
                        "createdTimestamp": 3,
                        "allowPublicRead" : false
                ]
        ]
        def mapBucket = {
            String id -> new Bucket([
                    identifier: id,
                    name: buckets[id]["name"] as String,
                    description: buckets[id]["description"] as String,
                    allowPublicRead: buckets[id]["allowPublicRead"]
            ]) }

        registryService.getBuckets() >> {[ mapBucket("b1"), mapBucket("b2"), mapBucket("b3") ]}

        def authorized = Mock(Authorizable)
        authorized.authorize(_, _, _) >> { return }
        def denied = Mock(Authorizable)
        denied.authorize(_, _, _) >> { throw new AccessDeniedException("") }

        authorizableLookup.getAuthorizableByResource("/actuator")   >> denied
        authorizableLookup.getAuthorizableByResource("/buckets")    >> authorized
        authorizableLookup.getAuthorizableByResource("/buckets/b1") >> authorized
        authorizableLookup.getAuthorizableByResource("/buckets/b2") >> authorized
        authorizableLookup.getAuthorizableByResource("/buckets/b3") >> denied
        authorizableLookup.getAuthorizableByResource("/policies")   >> authorized
        authorizableLookup.getAuthorizableByResource("/proxy")      >> denied
        authorizableLookup.getAuthorizableByResource("/swagger")    >> denied
        authorizableLookup.getAuthorizableByResource("/tenants")    >> authorized


        when:
        def resources = authorizationService.getAuthorizedResources(RequestAction.READ)

        then:
        resources != null
        resources.size() == 5
        def sortedResources = resources.sort{it.identifier}
        sortedResources[0].identifier == "/buckets"
        sortedResources[1].identifier == "/buckets/b1"
        sortedResources[2].identifier == "/buckets/b2"
        sortedResources[3].identifier == "/policies"
        sortedResources[4].identifier == "/tenants"


        when:
        def filteredResources = authorizationService.getAuthorizedResources(RequestAction.READ, ResourceType.Bucket)

        then:
        filteredResources != null
        filteredResources.size() == 3
        def sortedFilteredResources = filteredResources.sort{it.identifier}
        sortedFilteredResources[0].identifier == "/buckets"
        sortedFilteredResources[1].identifier == "/buckets/b1"
        sortedFilteredResources[2].identifier == "/buckets/b2"
    }

}
