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

class PermissionBasedStatusMergerSpec extends Specification {
    @Unroll
    def "Merge ConnectionStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, toMerge, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                                                                                                 |
                toMerge                                                                                                                                                                 ||
                expectedDto
        new ConnectionStatusDTO(canRead: true, groupId: 'real', id: 'real', name: 'real', sourceId: 'real', sourceName: 'real', destinationId: 'real', destinationName: 'real')                |
                new ConnectionStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                        destinationName: 'hidden')                                                                                                                                      ||
                new ConnectionStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                        destinationName: 'hidden')
        new ConnectionStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden', destinationName: 'hidden') |
                new ConnectionStatusDTO(canRead: true, groupId: 'real', id: 'real', name: 'real', sourceId: 'real', sourceName: 'real', destinationId: 'real', destinationName: 'real') ||
                new ConnectionStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden', destinationName: 'hidden')
    }

    @Unroll
    def "Merge ConnectionStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, toMerge)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                                                                                          |
                toMerge                                                                                                                                                                         ||
                expectedDto
        new ConnectionStatusSnapshotDTO(canRead: true, groupId: 'real', id: 'real', name: 'real', sourceId: 'real', sourceName: 'real', destinationId: 'real', destinationName: 'real') |
                new ConnectionStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                        destinationName: 'hidden')                                                                                                                                              ||
                new ConnectionStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                        destinationName: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: '0')
        new ConnectionStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                destinationName: 'hidden')                                                                                                                                              |
                new ConnectionStatusSnapshotDTO(canRead: true, groupId: 'real', id: 'real', name: 'real', sourceId: 'real', sourceName: 'real', destinationId: 'real', destinationName: 'real') ||
                new ConnectionStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', sourceId: 'hidden', sourceName: 'hidden', destinationId: 'hidden',
                        destinationName: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: '0')
    }

    @Unroll
    def "Merge PortStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, toMerge, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                             |
                toMerge                                                                            ||
                expectedDto
        new PortStatusDTO(canRead: true, groupId: 'real', id: 'real', name: 'real')        |
                new PortStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden') ||
                new PortStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden')
        new PortStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden') |
                new PortStatusDTO(canRead: true, groupId: 'real', id: 'real', name: 'real')        ||
                new PortStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden')
    }

    @Unroll
    def "Merge PortStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, toMerge)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                     |
                toMerge                                                                                    ||
                expectedDto
        new PortStatusSnapshotDTO(canRead: true, groupId: 'real', id: 'real', name: 'real')        |
                new PortStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden') ||
                new PortStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', transmitting: false)
        new PortStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden') |
                new PortStatusSnapshotDTO(canRead: true, groupId: 'real', id: 'real', name: 'real')        ||
                new PortStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', transmitting: false)
    }

    @Unroll
    def "Merge ProcessGroupStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, toMerge, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                  | toMerge                                                                 ||
                expectedDto
        new ProcessGroupStatusDTO(canRead: true, id: 'real', name: 'real')      | new ProcessGroupStatusDTO(canRead: false, id: 'hidden', name: 'hidden') ||
                new ProcessGroupStatusDTO(canRead: false, id: 'hidden', name: 'hidden')
        new ProcessGroupStatusDTO(canRead: false, id: 'hidden', name: 'hidden') | new ProcessGroupStatusDTO(canRead: true, id: 'real', name: 'real')      ||
                new ProcessGroupStatusDTO(canRead: false, id: 'hidden', name: 'hidden')
    }

    @Unroll
    def "Merge ProcessGroupStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, toMerge)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                          | toMerge                                                                         ||
                expectedDto
        new ProcessGroupStatusSnapshotDTO(canRead: true, id: 'real', name: 'real')      | new ProcessGroupStatusSnapshotDTO(canRead: false, id: 'hidden', name: 'hidden') ||
                new ProcessGroupStatusSnapshotDTO(canRead: false, id: 'hidden', name: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', transferred: '0 (0 bytes)', read: '0 bytes', written: '0' +
                        ' bytes',
                        queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: '0', received: '0 (0 bytes)', sent: '0 (0 bytes)', connectionStatusSnapshots: [], inputPortStatusSnapshots: [],
                        outputPortStatusSnapshots: [], processorStatusSnapshots: [], remoteProcessGroupStatusSnapshots: [])
        new ProcessGroupStatusSnapshotDTO(canRead: false, id: 'hidden', name: 'hidden') | new ProcessGroupStatusSnapshotDTO(canRead: true, id: 'real', name: 'real')      ||
                new ProcessGroupStatusSnapshotDTO(canRead: false, id: 'hidden', name: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', transferred: '0 (0 bytes)', read: '0 bytes', written: '0 bytes',
                        queued: '0 (0 bytes)', queuedSize: '0 bytes', queuedCount: '0', received: '0 (0 bytes)', sent: '0 (0 bytes)', connectionStatusSnapshots: [], inputPortStatusSnapshots: [],
                        outputPortStatusSnapshots: [], processorStatusSnapshots: [], remoteProcessGroupStatusSnapshots: [])
    }

    @Unroll
    def "Merge ProcessorStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, toMerge, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                  |
                toMerge                                                                                                 ||
                expectedDto
        new ProcessorStatusDTO(canRead: true, groupId: 'real', id: 'real', name: 'real', type: 'real')          |
                new ProcessorStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden') ||
                new ProcessorStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden')
        new ProcessorStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden') |
                new ProcessorStatusDTO(canRead: true, groupId: 'real', id: 'real', name: 'real', type: 'real')          ||
                new ProcessorStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden')
    }

    @Unroll
    def "Merge ProcessorStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, toMerge)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                          |
                toMerge                                                                                                         ||
                expectedDto
        new ProcessorStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden') |
                new ProcessorStatusSnapshotDTO(canRead: true, groupId: 'real', id: 'real', name: 'real', type: 'real')          ||
                new ProcessorStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', read: '0 bytes',
                        written: '0 bytes', tasks: '0', tasksDuration: '00:00:00.000')
        new ProcessorStatusSnapshotDTO(canRead: true, groupId: 'real', id: 'real', name: 'real', type: 'real')          |
                new ProcessorStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden') ||
                new ProcessorStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', type: 'hidden', input: '0 (0 bytes)', output: '0 (0 bytes)', read: '0 bytes',
                        written: '0 bytes', tasks: '0', tasksDuration: '00:00:00.000')
    }

    @Unroll
    def "Merge RemoteProcessGroupStatusDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, toMerge, 'nodeid', 'nodeaddress', 1234)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                                |
                toMerge                                                                                                               ||
                expectedDto
        new RemoteProcessGroupStatusDTO(canRead: true, groupId: 'real', id: 'real', name: 'real', targetUri: 'real')          |
                new RemoteProcessGroupStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden') ||
                new RemoteProcessGroupStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden')
        new RemoteProcessGroupStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden') |
                new RemoteProcessGroupStatusDTO(canRead: true, groupId: 'real', id: 'real', name: 'real', targetUri: 'real')          ||
                new RemoteProcessGroupStatusDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden')
    }

    @Unroll
    def "Merge RemoteProcessGroupStatusSnapshotDTO"() {
        given:
        def mapper = new ObjectMapper();
        def jaxbIntrospector = new JaxbAnnotationIntrospector();
        def SerializationConfig serializationConfig = mapper.getSerializationConfig();
        mapper.setSerializationConfig(serializationConfig.withSerializationInclusion(JsonSerialize.Inclusion.NON_NULL).withAnnotationIntrospector(jaxbIntrospector));
        def merger = new StatusMerger()

        when:
        merger.merge(target, toMerge)

        then:
        def returnedJson = mapper.writeValueAsString(target)
        def expectedJson = mapper.writeValueAsString(expectedDto)
        returnedJson == expectedJson

        where:
        target                                                                                                                                              |
                toMerge                                                                                                                       ||
                expectedDto
        new RemoteProcessGroupStatusSnapshotDTO(canRead: true, groupId: 'real', id: 'real', name: 'real', targetUri: 'real') |
                new RemoteProcessGroupStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden') ||
                new RemoteProcessGroupStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden', received: '0 (0 bytes)', sent: '0 (0 bytes)')
        new RemoteProcessGroupStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden')                       |
                new RemoteProcessGroupStatusSnapshotDTO(canRead: true, groupId: 'real', id: 'real', name: 'real', targetUri: 'real')          ||
                new RemoteProcessGroupStatusSnapshotDTO(canRead: false, groupId: 'hidden', id: 'hidden', name: 'hidden', targetUri: 'hidden', received: '0 (0 bytes)', sent: '0 (0 bytes)')
    }
}
