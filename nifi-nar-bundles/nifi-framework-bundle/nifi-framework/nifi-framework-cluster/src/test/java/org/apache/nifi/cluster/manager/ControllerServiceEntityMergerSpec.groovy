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

import org.apache.nifi.cluster.protocol.NodeIdentifier
import org.apache.nifi.controller.service.ControllerServiceState
import org.apache.nifi.web.api.dto.ConnectionDTO
import org.apache.nifi.web.api.dto.ControllerServiceDTO
import org.apache.nifi.web.api.dto.ControllerServiceReferencingComponentDTO
import org.apache.nifi.web.api.dto.PermissionsDTO
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO
import org.apache.nifi.web.api.entity.ConnectionEntity
import org.apache.nifi.web.api.entity.ControllerServiceEntity
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.SerializationConfig
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector
import spock.lang.Specification
import spock.lang.Unroll

@Unroll
class ControllerServiceEntityMergerSpec extends Specification {
    def "MergeComponents"() {
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector))
        def entity = nodeEntityMap.entrySet().first().value

        when:
        new ControllerServiceEntityMerger().merge(entity, nodeEntityMap)

        then:
        def mergedEntityJson = mapper.writeValueAsString(entity)
        def expectedJson = mapper.writeValueAsString(expectedMergedEntity)
        mergedEntityJson == expectedJson

        where:
        nodeEntityMap                                    ||
                expectedMergedEntity
        // Simple ControllerServiceEntity merging
        [(createNodeIdentifier(1)): new ControllerServiceEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true),
                component: new ControllerServiceDTO()),
         (createNodeIdentifier(2)): new ControllerServiceEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false),
                 component: new ControllerServiceDTO()),
         (createNodeIdentifier(3)): new ControllerServiceEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true),
                 component: new ControllerServiceDTO())] ||
                new ControllerServiceEntity(id: '1', permissions: new PermissionsDTO(canRead: false, canWrite: false))
        // Controller Reference merging for canRead==false
        [(createNodeIdentifier(1)): new ControllerServiceEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true),
                component: new ControllerServiceDTO(referencingComponents: [new ControllerServiceReferencingComponentEntity(permissions: new PermissionsDTO(canRead: true, canWrite: true),
                        component: new ControllerServiceReferencingComponentDTO(activeThreadCount: 1, state: ControllerServiceState.ENABLING.name()))])),
         (createNodeIdentifier(2)): new ControllerServiceEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true),
                 component: new ControllerServiceDTO(referencingComponents: [new ControllerServiceReferencingComponentEntity(permissions: new PermissionsDTO(canRead: false, canWrite: false),
                         component: new ControllerServiceReferencingComponentDTO(activeThreadCount: 1, state: ControllerServiceState.ENABLING.name()))])),
         (createNodeIdentifier(3)): new ControllerServiceEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true),
                 component: new ControllerServiceDTO(referencingComponents: [new ControllerServiceReferencingComponentEntity(permissions: new PermissionsDTO(canRead: true, canWrite: true),
                         component: new ControllerServiceReferencingComponentDTO(activeThreadCount: 1, state: ControllerServiceState.ENABLING.name()))]))] ||
                new ControllerServiceEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true),
                        bulletins: [],
                        component: new ControllerServiceDTO(validationErrors: [],
                                referencingComponents: [new ControllerServiceReferencingComponentEntity(permissions: new PermissionsDTO(canRead: false, canWrite: false))]))
        // Controller Reference merging for canRead==true
        [(createNodeIdentifier(1)): new ControllerServiceEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true),
                component: new ControllerServiceDTO(referencingComponents: [new ControllerServiceReferencingComponentEntity(permissions: new PermissionsDTO(canRead: true, canWrite: true),
                        component: new ControllerServiceReferencingComponentDTO(activeThreadCount: 1, state: ControllerServiceState.ENABLING.name()))])),
         (createNodeIdentifier(2)): new ControllerServiceEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true),
                 component: new ControllerServiceDTO(referencingComponents: [new ControllerServiceReferencingComponentEntity(permissions: new PermissionsDTO(canRead: true, canWrite: true),
                         component: new ControllerServiceReferencingComponentDTO(activeThreadCount: 1, state: ControllerServiceState.ENABLING.name()))])),
         (createNodeIdentifier(3)): new ControllerServiceEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true),
                 component: new ControllerServiceDTO(referencingComponents: [new ControllerServiceReferencingComponentEntity(permissions: new PermissionsDTO(canRead: true, canWrite: true),
                         component: new ControllerServiceReferencingComponentDTO(activeThreadCount: 1, state: ControllerServiceState.ENABLING.name()))]))] ||
                new ControllerServiceEntity(id: '1', permissions: new PermissionsDTO(canRead: true, canWrite: true),
                        bulletins: [],
                        component: new ControllerServiceDTO(validationErrors: [],
                                referencingComponents: [new ControllerServiceReferencingComponentEntity(permissions: new PermissionsDTO(canRead: true, canWrite: true),
                                        component: new ControllerServiceReferencingComponentDTO(activeThreadCount: 3, state: ControllerServiceState.ENABLING.name()))]))
    }

    def "MergeControllerServiceReferences"() {

    }

    def createNodeIdentifier(int id) {
        new NodeIdentifier("cluster-node-$id", 'addr', id, 'sktaddr', id * 10, 'stsaddr', id * 100, id * 1000, false, null)
    }
}
