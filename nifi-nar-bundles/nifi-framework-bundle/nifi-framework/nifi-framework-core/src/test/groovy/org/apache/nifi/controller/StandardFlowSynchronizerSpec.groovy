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
package org.apache.nifi.controller

import groovy.xml.XmlUtil
import org.apache.nifi.authorization.Authorizer
import org.apache.nifi.bundle.BundleCoordinate
import org.apache.nifi.cluster.protocol.DataFlow
import org.apache.nifi.connectable.*
import org.apache.nifi.controller.label.Label
import org.apache.nifi.controller.queue.FlowFileQueue
import org.apache.nifi.groups.ProcessGroup
import org.apache.nifi.groups.RemoteProcessGroup
import org.apache.nifi.nar.ExtensionManager
import org.apache.nifi.nar.SystemBundle
import org.apache.nifi.processor.Relationship
import org.apache.nifi.reporting.BulletinRepository
import org.apache.nifi.util.NiFiProperties
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

class StandardFlowSynchronizerSpec extends Specification {

    @Shared
    def systemBundle
    @Shared
    def nifiProperties

    def setupSpec() {
        def propFile = StandardFlowSynchronizerSpec.class.getResource("/standardflowsynchronizerspec.nifi.properties").getFile()

        nifiProperties = NiFiProperties.createBasicNiFiProperties(propFile, null)
        systemBundle = SystemBundle.create(nifiProperties)
        ExtensionManager.discoverExtensions(systemBundle, Collections.emptySet())
    }

    def teardownSpec() {

    }

    @Unroll
    def "scaling of #filename with encoding version \"#flowEncodingVersion\""() {
        given: "a StandardFlowSynchronizer with mocked collaborators"
        def controller = Mock FlowController
        def proposedFlow = Mock DataFlow
        def snippetManager = Mock SnippetManager
        def bulletinRepository = Mock BulletinRepository
        def flowFileQueue = Mock FlowFileQueue
        def authorizer = Mock Authorizer
        def flowFile = new File(StandardFlowSynchronizerSpec.getResource(filename).toURI())
        def flowControllerXml = new XmlSlurper().parse(flowFile)
        def Map<String, Position> originalPositionablePositionsById = flowControllerXml.rootGroup.'**'
                .findAll { !it.name().equals('connection') && it.id.size() == 1 && it.position.size() == 1 }
                .collectEntries { [it.id.text(), new Position(it.position.@x.toDouble(), it.position.@y.toDouble())] }
        def Map<String, List<Position>> originalBendPointsByConnectionId = flowControllerXml.rootGroup.'**'
                .findAll { it.name().equals('connection') && it.bendPoints.size() > 0 }
                .collectEntries { [it.id.text(), it.bendPoints.children().collect { new Position(it.@x.toDouble(), it.@y.toDouble()) }] }
        flowControllerXml.@'encoding-version' = flowEncodingVersion
        def testFlowBytes = XmlUtil.serialize(flowControllerXml).bytes
        def Map<String, Position> positionablePositionsById = [:]
        def Map<String, Positionable> positionableMocksById = [:]
        def Map<String, Connection> connectionMocksById = [:]
        def Map<String, List<Position>> bendPointPositionsByConnectionId = [:]
        // the unit under test
        def flowSynchronizer = new StandardFlowSynchronizer(null, nifiProperties)
        def firstRootGroup = Mock ProcessGroup

        when: "the flow is synchronized with the current state of the controller"
        flowSynchronizer.sync controller, proposedFlow, null

        then: "establish interactions for the mocked collaborators of StandardFlowSynchronizer to store the ending positions of components"
        1 * firstRootGroup.findAllProcessors() >> []
        1 * controller.isFlowSynchronized() >> false
        _ * controller.rootGroupId >> flowControllerXml.rootGroup.id.text()
        _ * controller.getGroup(_) >> { String id -> positionableMocksById.get(id) }
        _ * controller.snippetManager >> snippetManager
        _ * controller.bulletinRepository >> bulletinRepository
        _ * controller.authorizer >> authorizer
        _ * controller./set.*/(*_)
        _ * controller.getAllControllerServices() >> []
        _ * controller.getAllReportingTasks() >> []
        _ * controller.getRootGroup() >>> [
            firstRootGroup,
            positionableMocksById.get(controller.rootGroupId)
        ]
        _ * controller.createProcessGroup(_) >> { String pgId ->
            def processGroup = Mock(ProcessGroup)
            _ * processGroup.getIdentifier() >> pgId
            _ * processGroup.getPosition() >> { positionablePositionsById.get(pgId) }
            _ * processGroup.setPosition(_) >> { Position pos ->
                positionablePositionsById.put pgId, pos
            }
            _ * processGroup./(add|set).*/(*_)
            _ * processGroup.isEmpty() >> true
            _ * processGroup.isRootGroup() >> { pgId == flowControllerXml.rootGroup.id }
            _ * processGroup.getConnectable(_) >> { String connId -> positionableMocksById.get(connId) }
            _ * processGroup.findAllPositionables() >> {
                def foundProcessGroup = flowControllerXml.rootGroup.'**'.find { it.id == pgId }
                def idsUnderPg = foundProcessGroup.'**'.findAll { it.name() == 'id' }.collect { it.text() }
                positionableMocksById.entrySet().collect {
                    if (idsUnderPg.contains(it.key)) {
                        it.value
                    }
                }
            }
            _ * processGroup.findAllConnections() >> {
                def foundProcessGroup = flowControllerXml.rootGroup.'**'.find { it.id == pgId }
                def foundConnections = foundProcessGroup.'**'.findAll { it.name() == 'connection' }.collect { it.id.text() }
                connectionMocksById.entrySet().collect {
                    if (foundConnections.contains(it.key)) {
                        it.value
                    }
                }
            }
            positionableMocksById.put(pgId, processGroup)
            return processGroup
        }

        _ * controller.createProcessor(_, _, _, _) >> { String type, String id, BundleCoordinate coordinate, boolean firstTimeAdded ->
            def processor = Mock(ProcessorNode)
            _ * processor.getPosition() >> { positionablePositionsById.get(id) }
            _ * processor.setPosition(_) >> { Position pos ->
                positionablePositionsById.put id, pos
            }
            _ * processor./(add|set).*/(*_)
            _ * processor.getIdentifier() >> id
            _ * processor.getBundleCoordinate() >> coordinate
            _ * processor.getRelationship(_) >> { String n -> new Relationship.Builder().name(n).build() }
            positionableMocksById.put(id, processor)
            return processor
        }
        _ * controller.createFunnel(_) >> { String id ->
            def funnel = Mock(Funnel)
            _ * funnel.getPosition() >> { positionablePositionsById.get(id) }
            _ * funnel.setPosition(_) >> { Position pos ->
                positionablePositionsById.put id, pos
            }
            _ * funnel./(add|set).*/(*_)
            positionableMocksById.put id, funnel
            return funnel
        }
        _ * controller.createLabel(_, _) >> { String id, String text ->
            def l = Mock(Label)
            _ * l.getPosition() >> { positionablePositionsById.get(id) }
            _ * l.setPosition(_) >> { Position pos ->
                positionablePositionsById.put id, pos
            }
            _ * l./(add|set).*/(*_)
            positionableMocksById.put(id, l)
            return l
        }
        _ * controller./create.*Port/(_, _) >> { String id, String text ->
            def port = Mock(Port)
            _ * port.getPosition() >> { positionablePositionsById.get(id) }
            _ * port.setPosition(_) >> { Position pos ->
                positionablePositionsById.put id, pos
            }
            _ * port./(add|set).*/(*_)
            positionableMocksById.put(id, port)
            return port
        }
        _ * controller.createRemoteProcessGroup(_, _) >> { String id, String uri ->
            def rpg = Mock(RemoteProcessGroup)
            _ * rpg.getPosition() >> { positionablePositionsById.get(id) }
            _ * rpg.setPosition(_) >> { Position pos ->
                positionablePositionsById.put id, pos
            }
            _ * rpg./(add|set).*/(*_)
            _ * rpg.getOutputPort(_) >> { String rpgId -> positionableMocksById.get(rpgId) }
            _ * rpg.getIdentifier() >> id
            positionableMocksById.put(id, rpg)
            return rpg
        }
        _ * controller.createConnection(_, _, _, _, _) >> { String id, String name, Connectable source, Connectable destination, Collection<String> relationshipNames ->
            def connection = Mock(Connection)
            _ * connection.getIdentifier() >> id
            _ * connection.getBendPoints() >> {
                def bendpoints = bendPointPositionsByConnectionId.get(id)
                return bendpoints
            }
            _ * connection.setBendPoints(_) >> {
                // There seems to be a bug in Spock method matching where a list of arguments to a method
                // is being coerced into an Arrays$ArrayList with the actual list of bend points as an
                // ArrayList in the 0th element.
                // Need to keep an eye on this...
                bendPointPositionsByConnectionId.put id, it[0]
            }
            _ * connection./set.*/(*_)
            _ * connection.flowFileQueue >> flowFileQueue
            connectionMocksById.put(id, connection)
            return connection
        }
        _ * controller.startProcessor(*_)
        _ * controller.startConnectable(_)
        _ * controller.enableControllerServices(_)
        _ * snippetManager.export() >> {
            [] as byte[]
        }
        _ * snippetManager.clear()
        1 * proposedFlow.flow >> testFlowBytes
        _ * proposedFlow.snippets >> {
            [] as byte[]
        }
        _ * proposedFlow.authorizerFingerprint >> null
        _ * proposedFlow.missingComponents >> []

        _ * flowFileQueue./set.*/(*_)
        _ * _.hashCode() >> 1
        0 * _ // no other mock calls allowed

        then: "verify that the flow was scaled properly"
        originalPositionablePositionsById.entrySet().forEach { entry ->
            assert positionablePositionsById.containsKey(entry.key)
            def originalPosition = entry.value
            def position = positionablePositionsById.get(entry.key)
            compareOriginalPointToScaledPoint(originalPosition, position, isSyncedPositionGreater)
        }
        originalBendPointsByConnectionId.entrySet().forEach { entry ->
            assert bendPointPositionsByConnectionId.containsKey(entry.key)
            def originalBendPoints = entry.value
            def sortedBendPoints = bendPointPositionsByConnectionId.get(entry.key).sort { it.x }
            def sortedOriginalBendPoints = originalBendPoints.sort { it.x }
            assert sortedOriginalBendPoints.size() == sortedBendPoints.size()
            [sortedOriginalBendPoints, sortedBendPoints].transpose().forEach { Position originalPosition, Position position ->
                compareOriginalPointToScaledPoint(originalPosition, position, isSyncedPositionGreater)
            }
        }

        where: "the each flowfile and flow encoding version is run through the StandardFlowSynchronizer"
        filename                               | flowEncodingVersion | isSyncedPositionGreater
        '/conf/scale-positions-flow-0.7.0.xml' | null                | true
        '/conf/scale-positions-flow-0.7.0.xml' | '0.7'               | true
        '/conf/scale-positions-flow-0.7.0.xml' | '1.0'               | false
        '/conf/scale-positions-flow-0.7.0.xml' | '99.0'              | false
    }

    private void compareOriginalPointToScaledPoint(Position originalPosition, Position position, boolean isSyncedPositionGreater) {
        if (originalPosition.x == 0) {
            assert position.x == 0
        }
        if (originalPosition.y == 0) {
            assert position.y == 0
        }
        if (originalPosition.x > 0) {
            assert isSyncedPositionGreater == position.x > originalPosition.x
        }
        if (originalPosition.y > 0) {
            assert isSyncedPositionGreater == position.y > originalPosition.y
        }
        if (originalPosition.x < 0) {
            assert isSyncedPositionGreater == position.x < originalPosition.x
        }
        if (originalPosition.y < 0) {
            assert isSyncedPositionGreater == position.y < originalPosition.y
        }
    }
}
