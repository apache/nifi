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
package org.apache.nifi.util

import org.apache.nifi.web.api.dto.*
import spock.lang.Specification
import spock.lang.Unroll

class SnippetUtilsSpec extends Specification {
    @Unroll
    def "test moveAndScaleSnippet"() {
        given:
        def Map<ComponentDTO, PositionDTO> positions = (snippet.connections + snippet.inputPorts + snippet.outputPorts + snippet.labels + snippet.processGroups + snippet.processGroups +
                snippet.processors + snippet.funnels + snippet.remoteProcessGroups).collectEntries { ComponentDTO component ->
            [(component): new PositionDTO(component.position.x, component.position.y)]
        }

        when:
        SnippetUtils.moveAndScaleSnippet(snippet, nX, nY, fX, fY)

        then:
        positions.entrySet().forEach {
            def expectedPosition = calculateMoveAndScalePosition(it.value, oX, oY, nX, nY, fX, fY)
            def positionScaledBySnippetUtils = it.key.getPosition()
            assert positionScaledBySnippetUtils.x == expectedPosition.x
            assert positionScaledBySnippetUtils.y == expectedPosition.y
        }

        where:
        nX  | nY  | oX | oY | fX  | fY   | snippet
        500 | 500 | 0  | 0  | 1.5 | 1.34 | new FlowSnippetDTO()
        500 | 500 | 10 | 10 | 1.5 | 1.34 | new FlowSnippetDTO(processors: [new ProcessorDTO(id:"c81f6810-0155-1000-0000-c4af042cb1559", position: new PositionDTO(x: 10, y: 10))])
    }

    @Unroll
    def "test scaleSnippet"() {
        given:
        def Map<ComponentDTO, PositionDTO> positions = (snippet.connections + snippet.inputPorts + snippet.outputPorts + snippet.labels + snippet.processGroups + snippet.processGroups +
                snippet.processors + snippet.funnels + snippet.remoteProcessGroups).collectEntries { ComponentDTO component ->
            [(component): new PositionDTO(component.position.x, component.position.y)]
        }

        when:
        SnippetUtils.scaleSnippet(snippet, fX, fY)

        then:
        positions.entrySet().forEach {
            def expectedPosition = calculateScalePosition(it.value, fX, fY)
            def positionScaledBySnippetUtils = it.key.getPosition()
            assert positionScaledBySnippetUtils.x == expectedPosition.x
            assert positionScaledBySnippetUtils.y == expectedPosition.y
        }

        where:
        fX  | fY   | snippet
        1.5 | 1.34 | new FlowSnippetDTO()
        1.5 | 1.34 | new FlowSnippetDTO(
                processors: [new ProcessorDTO(id:"c81f6810-0155-1000-0001-c4af042cb1559", position: new PositionDTO(x: 10, y: 10))],
                processGroups: [
                        new ProcessGroupDTO(id:"c81f6a10-0155-1000-0002-c4af042cb1559", position: new PositionDTO(x: 105, y: -10), name: 'pg2',
                                contents: new FlowSnippetDTO(processors: [new ProcessorDTO(id:"c81f6810-0155-1000-0002-c4af042cb1559", name: 'proc1', position: new PositionDTO(x: 50, y: 60))]))])

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
