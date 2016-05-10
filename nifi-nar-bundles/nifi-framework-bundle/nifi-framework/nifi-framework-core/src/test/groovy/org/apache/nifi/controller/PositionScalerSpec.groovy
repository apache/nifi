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

import org.apache.nifi.connectable.Connectable
import org.apache.nifi.connectable.Position
import org.apache.nifi.connectable.Positionable
import spock.lang.Specification
import spock.lang.Unroll

class PositionScalerSpec extends Specification {

    @Unroll
    def "scale #positionableType.getSimpleName()"() {
        given:
        def positionable = Mock positionableType

        when:
        PositionScaler.scale positionable, factorX, factorY

        then:
        1 * positionable.position >> new Position(originalX, originalY)
        1 * positionable.setPosition(_) >> { Position p ->
            assert p.x == newX
            assert p.y == newY
        }

        where:
        positionableType | originalX | originalY | factorX | factorY | newX | newY
        Connectable      | 10        | 10        | 1.5     | 1.5     | 15   | 15
        Positionable     | -10       | -10       | 1.5     | 1.5     | -15  | -15
    }

    //TODO Test scaling of a ProcessGroup
}