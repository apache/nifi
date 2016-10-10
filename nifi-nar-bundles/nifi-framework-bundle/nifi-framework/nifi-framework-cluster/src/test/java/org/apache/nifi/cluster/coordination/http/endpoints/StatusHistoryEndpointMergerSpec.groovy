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
package org.apache.nifi.cluster.coordination.http.endpoints

import com.sun.jersey.api.client.ClientResponse
import org.apache.nifi.cluster.manager.NodeResponse
import org.apache.nifi.cluster.protocol.NodeIdentifier
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO
import org.apache.nifi.web.api.entity.StatusHistoryEntity
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.SerializationConfig
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector
import spock.lang.Specification
import spock.lang.Unroll

class StatusHistoryEndpointMergerSpec extends Specification {

    def setup() {
        def propFile = StatusHistoryEndpointMergerSpec.class.getResource("/conf/nifi.properties").getFile()
        System.setProperty NiFiProperties.PROPERTIES_FILE_PATH, propFile
    }

    def cleanup() {
        System.clearProperty NiFiProperties.PROPERTIES_FILE_PATH
    }

    @Unroll
    def "Merge component details based on permission"() {
        given: "json serialization setup"
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));

        and: "setup of the data to be used in the test"
        def merger = new StatusHistoryEndpointMerger(2)
        def requestUri = new URI("http://server/$requestUriPart")
        def requestId = UUID.randomUUID().toString()
        def Map<ClientResponse, Object> mockToRequestEntity = [:]
        def n = 0
        def nodeResponseSet = responseEntities.collect {
            ++n
            def clientResponse = Mock(ClientResponse)
            mockToRequestEntity.put clientResponse, it
            new NodeResponse(new NodeIdentifier("cluster-node-$n", 'addr', n, 'sktaddr', n * 10, 'stsaddr', n * 100, n * 1000, false, null), "get", requestUri, clientResponse, 500L, requestId)
        } as Set

        when:
        def returnedResponse = merger.merge(requestUri, httpMethod, nodeResponseSet, [] as Set, nodeResponseSet[0])

        then:
        mockToRequestEntity.entrySet().forEach {
            ClientResponse mockClientResponse = it.key
            def entity = it.value
            _ * mockClientResponse.getStatus() >> 200
            1 * mockClientResponse.getEntity(_) >> entity
        }
        responseEntities.size() == mockToRequestEntity.size()
        0 * _
        (returnedResponse.getUpdatedEntity() as StatusHistoryEntity).canRead == expectedEntity.canRead
        (returnedResponse.getUpdatedEntity() as StatusHistoryEntity).statusHistory.componentDetails == expectedEntity.statusHistory.componentDetails

        where:
        requestUriPart                                                   | httpMethod | responseEntities ||
                expectedEntity
        "/nifi-api/flow/connections/${UUID.randomUUID()}/status/history" | 'get'      | [
                new StatusHistoryEntity(canRead: true, statusHistory: new StatusHistoryDTO(componentDetails: [key1: 'real', key2: 'real'], nodeSnapshots: [], aggregateSnapshots: [])),
                new StatusHistoryEntity(canRead: false, statusHistory: new StatusHistoryDTO(componentDetails: [key1: 'hidden', key2: 'hidden'], nodeSnapshots: [], aggregateSnapshots: [])),
                new StatusHistoryEntity(canRead: true, statusHistory: new StatusHistoryDTO(componentDetails: [key1: 'real', key2: 'real'], nodeSnapshots: [], aggregateSnapshots: []))
        ]                                                                                                ||
                new StatusHistoryEntity(canRead: false, statusHistory: new StatusHistoryDTO(componentDetails: [key1: 'hidden', key2: 'hidden'], nodeSnapshots: [], aggregateSnapshots: []))
    }
}
