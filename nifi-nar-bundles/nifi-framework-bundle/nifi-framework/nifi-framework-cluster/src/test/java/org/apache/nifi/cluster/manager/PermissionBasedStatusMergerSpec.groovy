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
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO
import org.apache.nifi.web.api.dto.status.ConnectionStatusSnapshotDTO
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO
import org.apache.nifi.web.api.dto.status.PortStatusDTO
import org.apache.nifi.web.api.dto.status.PortStatusSnapshotDTO
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO
import org.apache.nifi.web.api.dto.status.ProcessorStatusSnapshotDTO
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusSnapshotDTO
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.SerializationConfig
import org.codehaus.jackson.map.annotate.JsonSerialize
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector
import spock.lang.Specification
import spock.lang.Unroll

@Unroll
class PermissionBasedStatusMergerSpec extends Specification {
    def "Merge ConnectionStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                                                                                 | targetCanRead |
                toMerge                                                                                                                                                  | toMergeCanRead ||
                expectedDto
        new ConnectionStatusDTO(groupId: 'real', id: 'real', name: 'real', sourceId: 'real', sourceName: 'real', destinationId: 'real', destinationName: 'real')               | true          |
                new ConnectionStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                        destinationName: 'hidden')                                                                                                                       | false          ||
                new ConnectionStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                        destinationName: 'hidden')
        new ConnectionStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden', destinationName: 'hidden') | false         |
                new ConnectionStatusDTO(groupId: 'real', id: 'real', name: 'real', sourceId: 'real', sourceName: 'real', destinationId: 'real', destinationName: 'real') | true           ||
                new ConnectionStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden', destinationName: 'hidden')
    }

    def "Merge ConnectionStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                                                                           | targetCanRead |
                toMerge                                                                                                                                                          | toMergeCanRead ||
                expectedDto
        new ConnectionStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real', sourceId: 'real', sourceName: 'real', destinationId: 'real', destinationName: 'real') | true          |
                new ConnectionStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                        destinationName: 'hidden')                                                                                                                               | false          ||
                new ConnectionStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                        destinationName: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: '0')
        new ConnectionStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                destinationName: 'hidden')                                                                                                                               | false         |
                new ConnectionStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real', sourceId: 'real', sourceName: 'real', destinationId: 'real', destinationName: 'real') | true           ||
                new ConnectionStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                        destinationName: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: '0')
    }

    def "Merge PortStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                             | targetCanRead |
                toMerge                                                            | toMergeCanRead ||
                expectedDto
        new PortStatusDTO(groupId: 'real', id: 'real', name: 'real', transmitting: 'false')       | true          |
                new PortStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', transmitting: 'false') | false          ||
                new PortStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', transmitting: 'false')
        new PortStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', transmitting: 'false') | false         |
                new PortStatusDTO(groupId: 'real', id: 'real', name: 'real', transmitting: 'false')       | true           ||
                new PortStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', transmitting: 'false')
    }

    def "Merge PortStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                     | targetCanRead |
                toMerge                                                                    | toMergeCanRead ||
                expectedDto
        new PortStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real')       | true          |
                new PortStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden') | false          ||
                new PortStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', transmitting: false)
        new PortStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden') | false         |
                new PortStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real')       | true           ||
                new PortStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', transmitting: false)
    }

    def "Merge ProcessGroupStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                  | targetCanRead |
                toMerge                                                                                                                   | toMergeCanRead ||
                expectedDto
        new ProcessGroupStatusDTO(id: 'real', name: 'real')     | true          | new ProcessGroupStatusDTO(id: 'hidden', name: 'hidden') | false          ||
                new ProcessGroupStatusDTO(id: 'hidden', name: 'hidden')
        new ProcessGroupStatusDTO(id: 'hidden', name: 'hidden') | false         | new ProcessGroupStatusDTO(id: 'real', name: 'real')     | true           ||
                new ProcessGroupStatusDTO(id: 'hidden', name: 'hidden')
    }

    def "Merge ProcessGroupStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                          | targetCanRead |
                toMerge                                                                                                                                   | toMergeCanRead ||
                expectedDto
        new ProcessGroupStatusSnapshotDTO(id: 'real', name: 'real')     | true          | new ProcessGroupStatusSnapshotDTO(id: 'hidden', name: 'hidden') | false          ||
                new ProcessGroupStatusSnapshotDTO(id: 'hidden', name: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', transferred: '0 (0 bytes)', read: '0 bytes', written: '0' +
                        ' bytes',
                        queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: '0', received: '0 (0 bytes)', sent: '0 (0 bytes)', connectionStatusSnapshots: [], inputPortStatusSnapshots: [],
                        outputPortStatusSnapshots: [], processorStatusSnapshots: [], remoteProcessGroupStatusSnapshots: [])
        new ProcessGroupStatusSnapshotDTO(id: 'hidden', name: 'hidden') | false         | new ProcessGroupStatusSnapshotDTO(id: 'real', name: 'real')     | true           ||
                new ProcessGroupStatusSnapshotDTO(id: 'hidden', name: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', transferred: '0 (0 bytes)', read: '0 bytes', written: '0 bytes',
                        queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: '0', received: '0 (0 bytes)', sent: '0 (0 bytes)', connectionStatusSnapshots: [], inputPortStatusSnapshots: [],
                        outputPortStatusSnapshots: [], processorStatusSnapshots: [], remoteProcessGroupStatusSnapshots: [])
    }

    def "Merge ProcessorStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                  | targetCanRead |
                toMerge                                                                                 | toMergeCanRead ||
                expectedDto
        new ProcessorStatusDTO(groupId: 'real', id: 'real', name: 'real', type: 'real')         | true          |
                new ProcessorStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden') | false          ||
                new ProcessorStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden')
        new ProcessorStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden') | false         |
                new ProcessorStatusDTO(groupId: 'real', id: 'real', name: 'real', type: 'real')         | true           ||
                new ProcessorStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden')
    }

    def "Merge ProcessorStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                          | targetCanRead |
                toMerge                                                                                         | toMergeCanRead ||
                expectedDto
        new ProcessorStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden') | false         |
                new ProcessorStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real', type: 'real')         | true           ||
                new ProcessorStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', read: '0 bytes',
                        written: '0 bytes', tasks: '0', tasksDuration: '00:00:00.000')
        new ProcessorStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real', type: 'real')         | true          |
                new ProcessorStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden') | false          ||
                new ProcessorStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', read: '0 bytes',
                        written: '0 bytes', tasks: '0', tasksDuration: '00:00:00.000')
    }

    def "Merge RemoteProcessGroupStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                | targetCanRead |
                toMerge                                                                                               | toMergeCanRead ||
                expectedDto
        new RemoteProcessGroupStatusDTO(groupId: 'real', id: 'real', name: 'real', targetUri: 'real')         | true          |
                new RemoteProcessGroupStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden') | false          ||
                new RemoteProcessGroupStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden')
        new RemoteProcessGroupStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden') | false         |
                new RemoteProcessGroupStatusDTO(groupId: 'real', id: 'real', name: 'real', targetUri: 'real')         | true           ||
                new RemoteProcessGroupStatusDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden')
    }

    def "Merge RemoteProcessGroupStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, targetCanRead, toMerge, toMergeCanRead)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                        | targetCanRead |
                toMerge                                                                                                       | toMergeCanRead ||
                expectedDto
        new RemoteProcessGroupStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real', targetUri: 'real')         | true          |
                new RemoteProcessGroupStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden') | false          ||
                new RemoteProcessGroupStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden', received: '0 (0 bytes)', sent: '0 (0 bytes)')
        new RemoteProcessGroupStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden') | false         |
                new RemoteProcessGroupStatusSnapshotDTO(groupId: 'real', id: 'real', name: 'real', targetUri: 'real')         | true           ||
                new RemoteProcessGroupStatusSnapshotDTO(groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden', received: '0 (0 bytes)', sent: '0 (0 bytes)')
    }
}
