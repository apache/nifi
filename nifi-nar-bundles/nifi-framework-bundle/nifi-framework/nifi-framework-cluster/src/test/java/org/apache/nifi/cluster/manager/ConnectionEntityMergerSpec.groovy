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
package org.apache.nifi.cluster.manager

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector
import org.apache.nifi.cluster.protocol.NodeIdentifier
import org.apache.nifi.web.api.dto.ConnectionDTO
import org.apache.nifi.web.api.dto.PermissionsDTO
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO
import org.apache.nifi.web.api.entity.ConnectionEntity
import spock.lang.Specification
import spock.lang.Unroll

class ConnectionEntityMergerSpec extends Specification {

    @Unroll
    def "Merge"() {
        given:
        def mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));

        def entity = nodeEntityMap.entrySet().first().value

        when:
        new ConnectionEntityMerger().merge(entity, nodeEntityMap)

        then:
        def mergedEntityJson = mapper.writeValueAsString(entity)
        def expectedJson = mapper.writeValueAsString(expectedMergedEntity)
        mergedEntityJson == expectedJson

        where:
        nodeEntityMap                                                                                                                    ||
                expectedMergedEntity
        [(createNodeIdentifier(1)): new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), status: new
                ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 300)), component: new ConnectionDTO()),
         (createNodeIdentifier(2)): new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false), status: new
                 ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 100))),
         (createNodeIdentifier(3)): new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true), status: new
                 ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 500)), component: new ConnectionDTO())] ||
                new ConnectionEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false),
                        status: new ConnectionStatusDTO(aggregateSnapshot: new ConnectionStatusSnapshotDTO(bytesIn: 900, input: '0 (900 bytes)',
                                output: '0 (0 bytes)', queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: 0)))

    }

    def createNodeIdentifier(int id) {
        new NodeIdentifier("cluster-node-$id", 'addr', id, 'sktaddr', id * 10, null, id * 10, 'stsaddr', id * 100, id * 1000, false, null)
    }
}
