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
package org.apache.nifi.cluster.coordination.http

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector
import org.apache.nifi.cluster.manager.NodeResponse
import org.apache.nifi.cluster.protocol.NodeIdentifier
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.web.api.dto.ConnectionDTO
import org.apache.nifi.web.api.dto.ControllerConfigurationDTO
import org.apache.nifi.web.api.dto.FunnelDTO
import org.apache.nifi.web.api.dto.LabelDTO
import org.apache.nifi.web.api.dto.PermissionsDTO
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO
import org.apache.nifi.web.api.entity.ConnectionEntity
import org.apache.nifi.web.api.entity.ConnectionsEntity
import org.apache.nifi.web.api.entity.ControllerConfigurationEntity
import org.apache.nifi.web.api.entity.FunnelEntity
import org.apache.nifi.web.api.entity.FunnelsEntity
import org.apache.nifi.web.api.entity.LabelEntity
import org.apache.nifi.web.api.entity.LabelsEntity
import spock.lang.Specification
import spock.lang.Unroll

import javax.ws.rs.core.Response
import java.text.NumberFormat

@Unroll
class StandardHttpResponseMapperSpec extends Specification {

    def setup() {
        def propFile = StandardHttpResponseMapperSpec.class.getResource("/conf/nifi.properties").getFile()
        System.setProperty NiFiProperties.PROPERTIES_FILE_PATH, propFile
    }

    def cleanup() {
        System.clearProperty NiFiProperties.PROPERTIES_FILE_PATH
    }

    def "MergeResponses: mixed HTTP GET response statuses, expecting #expectedStatus"() {
        given:
        def responseMapper = new StandardHttpResponseMapper(NiFiProperties.createBasicNiFiProperties(null,null))
        def requestUri = new URI('http://server/resource')
        def requestId = UUID.randomUUID().toString()
        def Map<Response, Map<String, Integer>> mockToRequestEntity = [:]
        def nodeResponseSet = nodeResponseData.collect {
            int n = it.node
            def response = Mock(Response)
            mockToRequestEntity.put response, it
            new NodeResponse(new NodeIdentifier("cluster-node-$n", 'addr', n, 'sktaddr', n * 10, 'sktaddr', n * 10, 'stsaddr', n * 100, n * 1000, false, null), "get", requestUri, response, 500L,
                    requestId)
        } as Set

        when:
        def returnedResponse = responseMapper.mapResponses(requestUri, 'get', nodeResponseSet, true).getStatus()

        then:
        mockToRequestEntity.entrySet().forEach {
            Response response = it.key
            _ * response.getStatus() >> it.value.status
        }
        0 * _
        returnedResponse == expectedStatus

        where:
        nodeResponseData                                                                || expectedStatus
        [[node: 1, status: 200], [node: 2, status: 200], [node: 3, status: 401]] as Set || 401
        [[node: 1, status: 200], [node: 2, status: 200], [node: 3, status: 403]] as Set || 403
        [[node: 1, status: 200], [node: 2, status: 403], [node: 3, status: 500]] as Set || 403
        [[node: 1, status: 200], [node: 2, status: 200], [node: 3, status: 500]] as Set || 500
    }

    def "MergeResponses: #responseEntities.size() HTTP 200 #httpMethod responses for #requestUriPart"() {
        given: "json serialization setup"
        def mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));

        and: "setup of the data to be used in the test"
        def responseMerger = new StandardHttpResponseMapper(NiFiProperties.createBasicNiFiProperties(null,null))
        def requestUri = new URI("http://server/$requestUriPart")
        def requestId = UUID.randomUUID().toString()
        def Map<Response, Object> mockToRequestEntity = [:]
        def n = 0
        def nodeResponseSet = responseEntities.collect {
            ++n
            def response = Mock(Response)
            mockToRequestEntity.put response, it
            new NodeResponse(new NodeIdentifier("cluster-node-$n", 'addr', n, 'sktaddr', n * 10, 'sktaddr', n * 11, 'stsaddr', n * 100, n * 1000, false, null), "get", requestUri, response, 500L,
                    requestId)
        } as Set

        when:
        def returnedResponse = responseMerger.mapResponses(requestUri, httpMethod, nodeResponseSet, true)

        then:
        mockToRequestEntity.entrySet().forEach {
            Response response = it.key
            def entity = it.value
            _ * response.getStatus() >> 200
            1 * response.readEntity(_) >> entity
        }
        responseEntities.size() == mockToRequestEntity.size()
        0 * _
        def returnedJson = mapper.writeValueAsString(returnedResponse.getUpdatedEntity())
        def expectedJson = mapper.writeValueAsString(expectedEntity)
        returnedJson == expectedJson

        where:
        requestUriPart                                             | httpMethod | responseEntities                                                                                     ||
                expectedEntity
        'nifi-api/controller/config'                               | 'get'      | [
                new ControllerConfigurationEntity(permissions: new PermissionsDTO(canRead: true, canWrite: true),
                        component: new ControllerConfigurationDTO(maxEventDrivenThreadCount: 10, maxTimerDrivenThreadCount: 10)),
                new ControllerConfigurationEntity(permissions: new PermissionsDTO(canRead: true, canWrite: false),
                        component: new ControllerConfigurationDTO(maxEventDrivenThreadCount: 10, maxTimerDrivenThreadCount: 10)),
                new ControllerConfigurationEntity(permissions: new PermissionsDTO(canRead: true, canWrite: true),
                        component: new ControllerConfigurationDTO(maxEventDrivenThreadCount: 10, maxTimerDrivenThreadCount: 10))]                                                      ||
                // expectedEntity
                new ControllerConfigurationEntity(permissions: new PermissionsDTO(canRead: true, canWrite: false),
                        component: new ControllerConfigurationDTO(maxEventDrivenThreadCount: 10, maxTimerDrivenThreadCount: 10))
        'nifi-api/controller/config'                               | 'put'      | [
                new ControllerConfigurationEntity(permissions: new PermissionsDTO(canRead: true, canWrite: true),
                        component: new ControllerConfigurationDTO(maxEventDrivenThreadCount: 10, maxTimerDrivenThreadCount: 10)),
                new ControllerConfigurationEntity(permissions: new PermissionsDTO(canRead: true, canWrite: false),
                        component: new ControllerConfigurationDTO(maxEventDrivenThreadCount: 10, maxTimerDrivenThreadCount: 10)),
                new ControllerConfigurationEntity(permissions: new PermissionsDTO(canRead: true, canWrite: true),
                        component: new ControllerConfigurationDTO(maxEventDrivenThreadCount: 10, maxTimerDrivenThreadCount: 10))]                                                      ||
                // expectedEntity
                new ControllerConfigurationEntity(permissions: new PermissionsDTO(canRead: true, canWrite: false),
                        component: new ControllerConfigurationDTO(maxEventDrivenThreadCount: 10, maxTimerDrivenThreadCount: 10))
        "nifi-api/process-groups/${UUID.randomUUID()}/connections" | 'get'      | [
                new ConnectionsEntity(connections: [new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), status: new
                        ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 300)), component: new ConnectionDTO())] as Set),
                new ConnectionsEntity(connections: [new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false), status: new
                        ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 100)))] as Set),
                new ConnectionsEntity(connections: [new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), status: new
                        ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 500)), component: new ConnectionDTO())] as Set)] ||
                // expectedEntity
                new ConnectionsEntity(connections: [new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false),
                        status: new ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 900,
                                input: '0 (900 bytes)', output: '0 (0 bytes)', queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: 0)))] as Set)
        "nifi-api/process-groups/${UUID.randomUUID()}/connections" | 'post'     | [
                new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), status:
                        new ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 300)), component: new ConnectionDTO()),
                new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false), status:
                        new ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 300))),
                new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), status:
                        new ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 300)), component: new ConnectionDTO())]      ||
                // expectedEntity
                new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false),
                        status: new ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 900, input: '0 (900 bytes)',
                                output: '0 (0 bytes)', queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: 0)))
        "nifi-api/connections/${UUID.randomUUID()}"                | 'get'      | [
                new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), status:
                        new ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 400)), component: new ConnectionDTO()),
                new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false), status:
                        new ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 300))),
                new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), status:
                        new ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 300)), component: new ConnectionDTO())]      ||
                // expectedEntity
                new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false),
                        status: new ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 1000,
                                input: "0 (${NumberFormat.instance.format(1000)} bytes)", output: '0 (0 bytes)', queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: 0)))
        "nifi-api/process-groups/${UUID.randomUUID()}/labels" | 'get'      | [
                new LabelsEntity(labels: [new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), component: new LabelDTO())] as Set),
                new LabelsEntity(labels: [new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false))] as Set),
                new LabelsEntity(labels: [new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), component: new LabelDTO())] as Set)] ||
                // expectedEntity
                new LabelsEntity(labels: [new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false))] as Set)
        "nifi-api/process-groups/${UUID.randomUUID()}/labels" | 'post'     | [
                new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), component: new LabelDTO()),
                new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false)),
                new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), component: new LabelDTO())]      ||
                // expectedEntity
                new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false))
        "nifi-api/labels/${UUID.randomUUID()}"                | 'get'      | [
                new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), component: new LabelDTO()),
                new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false)),
                new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), component: new LabelDTO())]      ||
                // expectedEntity
                new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false))
        "nifi-api/process-groups/${UUID.randomUUID()}/funnels" | 'get'      | [
                new FunnelsEntity(funnels: [new FunnelEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), component: new FunnelDTO())] as Set),
                new FunnelsEntity(funnels: [new FunnelEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false))] as Set),
                new FunnelsEntity(funnels: [new FunnelEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), component: new FunnelDTO())] as Set)] ||
                // expectedEntity
                new FunnelsEntity(funnels: [new FunnelEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false))] as Set)
        "nifi-api/process-groups/${UUID.randomUUID()}/funnels" | 'post'     | [
                new FunnelEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), component: new FunnelDTO()),
                new FunnelEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false)),
                new FunnelEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), component: new FunnelDTO())]      ||
                // expectedEntity
                new LabelEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false))
        "nifi-api/funnels/${UUID.randomUUID()}"                | 'get'      | [
                new FunnelEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), component: new FunnelDTO()),
                new FunnelEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false)),
                new FunnelEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), component: new FunnelDTO())]      ||
                // expectedEntity
                new FunnelEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false))
    }
}
