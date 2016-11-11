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
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.ZooKeeper
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

    def "Receive from open ZooKeeper without ACL migration"() {
        given:
        def server = new TestingServer()
        def client = new ZooKeeper(server.connectString, 3000, { WatchedEvent watchedEvent ->
        })
        def migrationPathRoot = '/nifi'
        client.create(migrationPathRoot, 'some data'.bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        def subPath = '1'
        client.create("$migrationPathRoot/$subPath", 'some data'.bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        subPath = '1/a'
        client.create("$migrationPathRoot/$subPath", 'some data'.bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        subPath = '2'
        client.create("$migrationPathRoot/$subPath", 'some data'.bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        subPath = '3'
        client.create("$migrationPathRoot/$subPath", 'some data'.bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        def outputFilePath = 'target/test-data.json'

        when:
        ZooKeeperMigratorMain.main(['-r', '-z', "$server.connectString$migrationPathRoot", '-f', outputFilePath] as String[])

        then:
        noExceptionThrown()
        def persistedData = new Gson().fromJson(new JsonReader(new FileReader(outputFilePath)), List) as List
        6 == persistedData.size();
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
        def nodes = getChildren(client, migrationPathRoot, [])
        6 == nodes.size()
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
        def nodes = getChildren(client, migrationPathRoot, [])
        6 == nodes.size()
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
        5 == persistedData.size();
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
        2 == persistedData.size();
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
        def nodes = getChildren(client, migrationPathRoot, [])
        2 == nodes.size()
    }

    def "Send to open Zookeeper root"() {
        given:
        def server = new TestingServer()
        def client = new ZooKeeper(server.connectString, 3000, { WatchedEvent watchedEvent ->
        })
        def migrationPathRoot = '/'

        when:
        ZooKeeperMigratorMain.main(['-s', '-z', "$server.connectString$migrationPathRoot", '-f', 'src/test/resources/test-data-user-pass.json'] as String[])

        then:
        noExceptionThrown()
        def nodes = getChildren(client, migrationPathRoot, [])
        4 == nodes.size()
    }

    def "Parse Zookeeper connect string and path"() {
        when:
        def zooKeeperMigrator = new ZooKeeperMigrator("$connectStringAndPath")
        def tokens = connectStringAndPath.split('/', 2) as List
        def connectString = tokens[0]
        def path = '/' + (tokens.size() > 1 ? tokens[1] : '')

        then:
        connectString == zooKeeperMigrator.getZooKeeperEndpointConfig().connectString
        path == zooKeeperMigrator.getZooKeeperEndpointConfig().path

        where:
        connectStringAndPath       || _
        '127.0.0.1'                || _
        '127.0.0.1/'               || _
        '127.0.0.1:2181'           || _
        '127.0.0.1:2181/'          || _
        '127.0.0.1/path'           || _
        '127.0.0.1/path/node'      || _
        '127.0.0.1:2181/'          || _
        '127.0.0.1:2181/path'      || _
        '127.0.0.1:2181/path/node' || _
    }

    def List<String> getChildren(ZooKeeper client, String path, List<String> ag) {
        def children = client.getChildren(path, null)
        ag.add path
        children.forEach {
            def childPath = "/${(path.tokenize('/') + it).join('/')}"
            getChildren(client, childPath, ag)
        }
        ag
    }
}
