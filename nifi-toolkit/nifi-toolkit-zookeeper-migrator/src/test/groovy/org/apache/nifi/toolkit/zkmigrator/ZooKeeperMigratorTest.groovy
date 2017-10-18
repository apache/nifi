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
package org.apache.nifi.toolkit.zkmigrator

import com.google.gson.Gson
import com.google.gson.stream.JsonReader
import org.apache.curator.test.TestingServer
import org.apache.curator.utils.ZKPaths
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.ZKUtil
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.charset.StandardCharsets

@Unroll
class ZooKeeperMigratorTest extends Specification {

    def "Test auth and jaas usage simultaneously"() {
        when:
        ZooKeeperMigratorMain.main(['-r', '-z', 'localhost:2181/path', '-a', 'user:pass', '-k', 'jaas.conf'] as String[])

        then:
        noExceptionThrown()
    }

    @Ignore
    def "Test jaas conf on command line"() {
        when:
        ZooKeeperMigratorMain.main(['-r', '-z', 'localhost:2181/path', '-k', 'jaas.conf'] as String[])

        then:
        noExceptionThrown()
    }

    def "Receive from open ZooKeeper"() {
        given:
        def server = new TestingServer()
        def client = new ZooKeeper(server.connectString, 3000, { WatchedEvent watchedEvent ->
        })
        def migrationPathRoot = '/nifi/components'
        ZKPaths.mkdirs(client, migrationPathRoot)
        client.setData(migrationPathRoot, 'some data'.bytes, 0)
        def componentName = '1'
        client.create("$migrationPathRoot/$componentName", 'some data'.bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        componentName = '1/a'
        client.create("$migrationPathRoot/$componentName", 'some data'.bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        componentName = '2'
        client.create("$migrationPathRoot/$componentName", 'some data'.bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        componentName = '3'
        client.create("$migrationPathRoot/$componentName", 'some data'.bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        def outputFilePath = 'target/test-data.json'

        when:
        ZooKeeperMigratorMain.main(['-r', '-z', "$server.connectString$migrationPathRoot", '-f', outputFilePath] as String[])

        then:
        noExceptionThrown()
        def persistedData = new Gson().fromJson(new JsonReader(new FileReader(outputFilePath)), List) as List
        persistedData.size() == 6
    }

    def "Send to open ZooKeeper without ACL migration"() {
        given:
        def server = new TestingServer()
        def client = new ZooKeeper(server.connectString, 3000, { WatchedEvent watchedEvent ->
        })
        def migrationPathRoot = '/newParent'

        when:
        ZooKeeperMigratorMain.main(['-s', '-z', "$server.connectString$migrationPathRoot", '-f', 'src/test/resources/test-data.json'] as String[])

        then:
        noExceptionThrown()
        def nodes = ZKPaths.getSortedChildren(client, '/').collect { ZKUtil.listSubTreeBFS(client, "/$it") }.flatten()
        nodes.size() == 6
    }

    def "Send to open ZooKeeper without ACL migration with new multi-node parent"() {
        given:
        def server = new TestingServer()
        def client = new ZooKeeper(server.connectString, 3000, { WatchedEvent watchedEvent ->
        })
        def migrationPathRoot = '/newParent/node'

        when:
        ZooKeeperMigratorMain.main(['-s', '-z', "$server.connectString$migrationPathRoot", '-f', 'src/test/resources/test-data.json'] as String[])

        then:
        noExceptionThrown()
        def nodes = ZKPaths.getSortedChildren(client, '/').collect { ZKUtil.listSubTreeBFS(client, "/$it") }.flatten()
        nodes.size() == 7
    }

    def "Receive all nodes from ZooKeeper root"() {
        given:
        def server = new TestingServer()
        def client = new ZooKeeper(server.connectString, 3000, { WatchedEvent watchedEvent ->
        })
        def migrationPathRoot = '/'
        def addedNodePath = 'nifi'
        client.create("$migrationPathRoot$addedNodePath", 'some data'.bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        def outputFilePath = 'target/test-data-root.json'

        when:
        ZooKeeperMigratorMain.main(['-r', '-z', "$server.connectString$migrationPathRoot", '-f', outputFilePath] as String[])

        then:
        noExceptionThrown()
        def persistedData = new Gson().fromJson(new JsonReader(new FileReader(outputFilePath)), List) as List
        persistedData.size() == 5
    }

    def "Receive Zookeeper node created with username and password"() {
        given:
        def server = new TestingServer()
        def client = new ZooKeeper(server.connectString, 3000, { WatchedEvent watchedEvent ->
        })
        def username = 'nifi'
        def password = 'nifi'
        client.addAuthInfo('digest', "$username:$password".getBytes(StandardCharsets.UTF_8))
        def migrationPathRoot = '/nifi'
        client.create(migrationPathRoot, 'some data'.bytes, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT)
        def outputFilePath = 'target/test-data-user-pass.json'

        when:
        ZooKeeperMigratorMain.main(['-r', '-z', "$server.connectString$migrationPathRoot", '-f', outputFilePath, '-a', "$username:$password"] as String[])

        then:
        noExceptionThrown()
        def persistedData = new Gson().fromJson(new JsonReader(new FileReader(outputFilePath)), List) as List
        persistedData.size() == 2
    }

    def "Send to Zookeeper a node created with username and password"() {
        given:
        def server = new TestingServer()
        def client = new ZooKeeper(server.connectString, 3000, { WatchedEvent watchedEvent ->
        })
        def username = 'nifi'
        def password = 'nifi'
        client.addAuthInfo('digest', "$username:$password".getBytes(StandardCharsets.UTF_8))
        def migrationPathRoot = '/newParent'

        when:
        ZooKeeperMigratorMain.main(['-s', '-z', "$server.connectString$migrationPathRoot", '-f', 'src/test/resources/test-data-user-pass.json', '-a', "$username:$password"] as String[])

        then:
        noExceptionThrown()
        def nodes = ZKPaths.getSortedChildren(client, '/').collect { ZKUtil.listSubTreeBFS(client, "/$it") }.flatten()
        nodes.size() == 3
    }

    def "Send to open Zookeeper with ACL migration"() {
        given:
        def server = new TestingServer()
        def client = new ZooKeeper(server.connectString, 3000, { WatchedEvent watchedEvent ->
        })
        def migrationPathRoot = '/nifi-open'

        when:
        ZooKeeperMigratorMain.main(['-s', '-z', "$server.connectString$migrationPathRoot", '-f', 'src/test/resources/test-data-user-pass.json'] as String[])

        then:
        noExceptionThrown()
        def nodes = ZKPaths.getSortedChildren(client, '/').collect { ZKUtil.listSubTreeBFS(client, "/$it") }.flatten()
        nodes.size() == 3
    }

    def "Send to open Zookeeper using existing ACL"() {
        given:
        def server = new TestingServer()
        def securedClient = new ZooKeeper(server.connectString, 3000, { WatchedEvent watchedEvent -> })
        def userPass = "nifi:nifi"
        securedClient.addAuthInfo("digest",userPass.getBytes(StandardCharsets.UTF_8))
        def digest = DigestAuthenticationProvider.generateDigest(userPass)
        def migrationPathRoot = '/nifi'
        def stat = new Stat()

        when:
        ZooKeeperMigratorMain.main(['-s', '-z', "$server.connectString$migrationPathRoot", '-f', 'src/test/resources/test-data-user-pass.json','--use-existing-acl'] as String[])

        then:
        noExceptionThrown()
        def acl = securedClient.getACL("/nifi",stat)
        acl.get(0).id.scheme == "digest"
        acl.get(0).id.id == digest
        def nodes = ZKPaths.getSortedChildren(securedClient, '/nifi').collect { ZKUtil.listSubTreeBFS(securedClient, "/$it") }.flatten()
        nodes.size() == 0
    }


    def "Parse Zookeeper connect string and path"() {
        when:
        def zooKeeperMigrator = new ZooKeeperMigrator("$connectString")

        then:
        zooKeeperMigrator.zooKeeperEndpointConfig.connectString == connectString
        zooKeeperMigrator.zooKeeperEndpointConfig.servers == servers.split(',').collect()
        zooKeeperMigrator.zooKeeperEndpointConfig.path == path

        where:
        connectString                                       | path         | servers                                   || _
        '127.0.0.1'                                         | '/'          | '127.0.0.1'                               || _
        '127.0.0.1,127.0.0.2'                               | '/'          | '127.0.0.1,127.0.0.2'                     || _
        '127.0.0.1/'                                        | '/'          | '127.0.0.1'                               || _
        '127.0.0.1,127.0.0.2/'                              | '/'          | '127.0.0.1,127.0.0.2'                     || _
        '127.0.0.1:2181'                                    | '/'          | '127.0.0.1:2181'                          || _
        '127.0.0.1,127.0.0.2:2181'                          | '/'          | '127.0.0.1,127.0.0.2:2181'                || _
        '127.0.0.1:2181/'                                   | '/'          | '127.0.0.1:2181'                          || _
        '127.0.0.1,127.0.0.2:2181/'                         | '/'          | '127.0.0.1,127.0.0.2:2181'                || _
        '127.0.0.1/path'                                    | '/path'      | '127.0.0.1'                               || _
        '127.0.0.1,127.0.0.2/path'                          | '/path'      | '127.0.0.1,127.0.0.2'                     || _
        '127.0.0.1/path/node'                               | '/path/node' | '127.0.0.1'                               || _
        '127.0.0.1,127.0.0.2/path/node'                     | '/path/node' | '127.0.0.1,127.0.0.2'                     || _
        '127.0.0.1:2181/'                                   | '/'          | '127.0.0.1:2181'                          || _
        '127.0.0.1,127.0.0.2:2181/'                         | '/'          | '127.0.0.1,127.0.0.2:2181'                || _
        '127.0.0.1:2181/path'                               | '/path'      | '127.0.0.1:2181'                          || _
        '127.0.0.1,127.0.0.2:2181/path'                     | '/path'      | '127.0.0.1,127.0.0.2:2181'                || _
        '127.0.0.1:2181/path/node'                          | '/path/node' | '127.0.0.1:2181'                          || _
        '127.0.0.1,127.0.0.2:2181/path/node'                | '/path/node' | '127.0.0.1,127.0.0.2:2181'                || _
        '127.0.0.1,127.0.0.2:2182,127.0.0.3:2183'           | '/'          | '127.0.0.1,127.0.0.2:2182,127.0.0.3:2183' || _
        '127.0.0.1,127.0.0.2:2182,127.0.0.3:2183/'          | '/'          | '127.0.0.1,127.0.0.2:2182,127.0.0.3:2183' || _
        '127.0.0.1,127.0.0.2:2182,127.0.0.3:2183/path'      | '/path'      | '127.0.0.1,127.0.0.2:2182,127.0.0.3:2183' || _
        '127.0.0.1,127.0.0.2:2182,127.0.0.3:2183/path/node' | '/path/node' | '127.0.0.1,127.0.0.2:2182,127.0.0.3:2183' || _
    }

    def "Test ignore source"() {
        given:
        def server = new TestingServer()
        def connectString = "$server.connectString"
        def dataPath = 'target/test-data-ignore-source.json'

        when: "data is read from the source zookeeper"
        ZooKeeperMigratorMain.main(['-r', '-z', connectString, '-f', dataPath] as String[])

        then: "verify the data has been written to the output file"
        new File(dataPath).exists()

        when: "data is sent to the same zookeeper as the the source zookeeper without ignore source"
        ZooKeeperMigratorMain.main(['-s', '-z', connectString, '-f', dataPath] as String[])

        then: "verify that an illegal argument exception is thrown"
        thrown(IllegalArgumentException)

        when: "data is sent to the same zookeeper as the source zookeeper with ignore source option is set"
        ZooKeeperMigratorMain.main(['-s', '-z', connectString, '-f', dataPath, '--ignore-source'] as String[])

        then: "no exceptions are thrown"
        noExceptionThrown()
    }

    def "Send to same ZooKeeper with different path"() {
        def server = new TestingServer()
        def connectString = "$server.connectString"
        def dataPath = 'target/test-data-different-path.json'

        when: "data is read from the source zookeeper"
        ZooKeeperMigratorMain.main(['-r', '-z', connectString, '-f', dataPath] as String[])

        then: "verify the data has been written to the output file"
        new File(dataPath).exists()

        when: "data is sent to the same zookeeper as the the source zookeeper with a different path"
        ZooKeeperMigratorMain.main(['-s', '-z', "$connectString/new-path", '-f', dataPath] as String[])

        then: "no exceptions are thrown"
        noExceptionThrown()
    }
}
