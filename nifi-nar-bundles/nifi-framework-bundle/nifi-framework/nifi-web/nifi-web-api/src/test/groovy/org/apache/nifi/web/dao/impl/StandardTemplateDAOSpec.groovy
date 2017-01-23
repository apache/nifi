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
package org.apache.nifi.web.dao.impl

import org.apache.nifi.authorization.Authorizer
import org.apache.nifi.controller.FlowController
import org.apache.nifi.controller.serialization.FlowEncodingVersion
import org.apache.nifi.controller.service.ControllerServiceProvider
import org.apache.nifi.groups.ProcessGroup
import org.apache.nifi.web.api.dto.BundleDTO
import org.apache.nifi.web.api.dto.ComponentDTO
import org.apache.nifi.web.api.dto.DtoFactory
import org.apache.nifi.web.api.dto.FlowSnippetDTO
import org.apache.nifi.web.api.dto.PositionDTO
import org.apache.nifi.web.api.dto.ProcessGroupDTO
import org.apache.nifi.web.api.dto.ProcessorConfigDTO
import org.apache.nifi.web.api.dto.ProcessorDTO
import org.apache.nifi.web.util.SnippetUtils
import spock.lang.Specification
import spock.lang.Unroll

class StandardTemplateDAOSpec extends Specification {

    @Unroll
    def "test InstantiateTemplate moves and scales templates"() {
        given:
        def flowController = Mock FlowController
        def snippetUtils = new SnippetUtils()
        snippetUtils.flowController = flowController
        def dtoFactory = new DtoFactory()
        dtoFactory.authorizer = Mock Authorizer
        dtoFactory.controllerServiceProvider = Mock ControllerServiceProvider
        snippetUtils.dtoFactory = dtoFactory
        def standardTemplateDAO = new StandardTemplateDAO()
        standardTemplateDAO.flowController = flowController
        standardTemplateDAO.snippetUtils = snippetUtils
        def templateEncodingVersion = FlowEncodingVersion.parse(encodingVersion);
        // get the major version, or 0 if no version could be parsed
        int templateEncodingMajorVersion = templateEncodingVersion != null ? templateEncodingVersion.getMajorVersion() : 0;
        double factorX = templateEncodingMajorVersion < 1 ? FlowController.DEFAULT_POSITION_SCALE_FACTOR_X : 1.0;
        double factorY = templateEncodingMajorVersion < 1 ? FlowController.DEFAULT_POSITION_SCALE_FACTOR_Y : 1.0;
        // get all top-level component starting positions
        def List<ComponentDTO> components = [snippet.connections + snippet.inputPorts + snippet.outputPorts + snippet.labels + snippet.processGroups + snippet.processGroups +
                                                     snippet.processors + snippet.funnels + snippet.remoteProcessGroups].flatten()

        // get all starting subcomponent starting positions
        def List<ComponentDTO> subComponents = org.apache.nifi.util.SnippetUtils.findAllProcessGroups(snippet).collect { ProcessGroupDTO processGroup ->
            def childSnippet = processGroup.contents
            childSnippet.connections + childSnippet.inputPorts + childSnippet.outputPorts + childSnippet.labels + childSnippet.processGroups + childSnippet.processGroups +
                    childSnippet.processors + childSnippet.funnels + childSnippet.remoteProcessGroups
        }.flatten()

        when:
        def instantiatedTemplate = standardTemplateDAO.instantiateTemplate(rootGroupId, newOriginX, newOriginY, encodingVersion, snippet, idGenerationSeed)

        then:
        flowController.getGroup(_) >> { String gId ->
            def pg = Mock ProcessGroup
            pg.identifier >> gId
            pg.inputPorts >> []
            pg.outputPorts >> []
            pg.processGroups >> []
            return pg
        }
        flowController.rootGroupId >> rootGroupId
        flowController.instantiateSnippet(*_) >> {}
        0 * _

        def instantiatedComponents = [instantiatedTemplate.connections + instantiatedTemplate.inputPorts + instantiatedTemplate.outputPorts + instantiatedTemplate.labels +
                                              instantiatedTemplate.processGroups + instantiatedTemplate.processGroups + instantiatedTemplate.processors + instantiatedTemplate.funnels +
                                              instantiatedTemplate.remoteProcessGroups].flatten()
        components.forEach { component ->
            def correspondingScaledPosition = instantiatedComponents.find { scaledComponent ->
                scaledComponent.name.equals(component.name)
            }.position
            assert correspondingScaledPosition != null
            def expectedPosition = calculateMoveAndScalePosition(component.position, oldOriginX, oldOriginY, newOriginX, newOriginY, factorX, factorY)
            assert correspondingScaledPosition.x == expectedPosition.x
            assert correspondingScaledPosition.y == expectedPosition.y

        }
        def instantiatedSubComponents = org.apache.nifi.util.SnippetUtils.findAllProcessGroups(instantiatedTemplate).collect { ProcessGroupDTO processGroup ->
            def childSnippet = processGroup.contents
            childSnippet.connections + childSnippet.inputPorts + childSnippet.outputPorts + childSnippet.labels + childSnippet.processGroups + childSnippet.processGroups +
                    childSnippet.processors + childSnippet.funnels + childSnippet.remoteProcessGroups
        }.flatten()
        subComponents.forEach { subComponent ->
            def correspondingScaledPosition = instantiatedSubComponents.find { scaledComponent ->
                scaledComponent.name.equals(subComponent.name)
            }.position
            assert correspondingScaledPosition != null
            def expectedPosition = calculateScalePosition(subComponent.position, factorX, factorY)
            assert correspondingScaledPosition.x == expectedPosition.x
            assert correspondingScaledPosition.y == expectedPosition.y

        }

        where:
        rootGroupId | oldOriginX | oldOriginY | newOriginX | newOriginY | idGenerationSeed | encodingVersion | snippet
        'g1'        | 0.0        | 0.0        | 5.0        | 5.0        | 'AAAA'           | null            | new FlowSnippetDTO()
        'g1'        | 10.0       | 10.0       | 5.0        | 5.0        | 'AAAA'           | '0.7'           | new FlowSnippetDTO(
                processors: [new ProcessorDTO(id:"c81f6810-0155-1000-0000-c4af042cb1559", name: 'proc1', bundle: new BundleDTO("org.apache.nifi", "standard", "1.0"),
                        config: new ProcessorConfigDTO(), position: new PositionDTO(x: 10, y: 10))])
        'g1'        | 10.0       | -10.0      | 5.0        | 5.0        | 'AAAA'           | null           | new FlowSnippetDTO(
                processors: [new ProcessorDTO(id:"c81f6810-0155-1000-0001-c4af042cb1559", name: 'proc2', bundle: new BundleDTO("org.apache.nifi", "standard", "1.0"),
                        config: new ProcessorConfigDTO(), position: new PositionDTO(x: 10, y: 10))],
                processGroups: [
                        new ProcessGroupDTO(id:"c81f6810-0a55-1000-0000-c4af042cb1559", 
                                name: 'g2',
                                position: new PositionDTO(x: 105, y: -10),
                                contents: new FlowSnippetDTO(processors: [new ProcessorDTO(id:"c81f6810-0155-1000-0002-c4af042cb1559", name: 'proc3', bundle: new BundleDTO("org.apache.nifi", "standard", "1.0"),
                                        config: new ProcessorConfigDTO(), position: new PositionDTO(x: 50, y: 60))]))])
        'g1'        | 10.0       | -10.0      | 5.0        | 5.0        | 'AAAA'           | '0.7'           | new FlowSnippetDTO(
                processors: [new ProcessorDTO(id:"c81f6810-0155-1000-0003-c4af042cb1559", name: 'proc2', bundle: new BundleDTO("org.apache.nifi", "standard", "1.0"),
                        config: new ProcessorConfigDTO(), position: new PositionDTO(x: 10, y: 10))],
                processGroups: [
                        new ProcessGroupDTO(id:"c81f6810-0a55-1000-0001-c4af042cb1559",
                                name: 'g2',
                                position: new PositionDTO(x: 105, y: -10),
                                contents: new FlowSnippetDTO(processors: [new ProcessorDTO(id:"c81f6810-0155-1000-0004-c4af042cb1559", name: 'proc3', bundle: new BundleDTO("org.apache.nifi", "standard", "1.0"),
                                        config: new ProcessorConfigDTO(), position: new PositionDTO(x: 50, y: 60))]))])
        'g1'        | 10.0       | -10.0      | 5.0        | 5.0        | 'AAAA'           | '1.0'           | new FlowSnippetDTO(
                processors: [new ProcessorDTO(id:"c81f6810-0155-1000-0005-c4af042cb1559", name: 'proc2', bundle: new BundleDTO("org.apache.nifi", "standard", "1.0"),
                        config: new ProcessorConfigDTO(), position: new PositionDTO(x: 10, y: 10))],
                processGroups: [
                        new ProcessGroupDTO(id:"c81f6810-0a55-1000-0003-c4af042cb1559",
                                name: 'g2',
                                position: new PositionDTO(x: 105, y: -10),
                                contents: new FlowSnippetDTO(processors: [new ProcessorDTO(id:"c81f6810-0155-1000-0006-c4af042cb1559", name: 'proc3', bundle: new BundleDTO("org.apache.nifi", "standard", "1.0"),
                                        config: new ProcessorConfigDTO(), position: new PositionDTO(x: 50, y: 60))]))])
    }

    def PositionDTO calculateMoveAndScalePosition(position, oldOriginX, oldOriginY, newOriginX, newOriginY, factorX, factorY) {
        new PositionDTO(
                x: newOriginX + (position.x - oldOriginX) * factorX,
                y: newOriginY + (position.y - oldOriginY) * factorY)
    }

    def PositionDTO calculateScalePosition(position, factorX, factorY) {
        new PositionDTO(
                x: position.x * factorX,
                y: position.y * factorY)
    }
}