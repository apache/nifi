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
package org.apache.nifi.controller;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Positionable;
import org.apache.nifi.groups.ProcessGroup;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides utility to scale the positions of {@link Positionable}s and bend points of {@link Connection}s
 * by a given factor.
 */
class PositionScaler {

    /**
     * Scales the positions of all {@link Position}s in the given {@link ProcessGroup} by
     * the provided factor.  This method replaces all {@link Position}s in each {@link Positionable}
     * in the {@link ProcessGroup} with a new scaled {@link Position}.
     *
     * @param processGroup containing the {@link Positionable}s to be scaled
     * @param factorX      used to scale a {@link Positionable}'s X-coordinate position
     * @param factorY      used to scale a {@link Positionable}'s Y-coordinate position
     */
    public static void scale(ProcessGroup processGroup, double factorX, double factorY) {
        processGroup.findAllPositionables().stream().forEach(p -> scale(p, factorX, factorY));
        Map<Connection, List<Position>> bendPointsByConnection =
                processGroup.findAllConnections().stream().collect(Collectors.toMap(connection -> connection, Connection::getBendPoints));
        bendPointsByConnection.entrySet().stream()
                .forEach(connectionListEntry -> connectionListEntry.getKey().setBendPoints(connectionListEntry.getValue().stream()
                        .map(p -> scalePosition(p, factorX, factorY)).collect(Collectors.toList())));

    }

    /**
     * Scales the {@link Position} of the given {@link Positionable} by the provided factor.  This method
     * replaces the {@link Position} in the {@link Positionable} with a new scaled {@link Position}.
     *
     * @param positionable containing a {@link Position} to scale
     * @param factorX      used to scale a {@link Positionable}'s X-coordinate position
     * @param factorY      used to scale a {@link Positionable}'s Y-coordinate position
     */
    public static void scale(Positionable positionable, double factorX, double factorY) {
        final Position startingPosition = positionable.getPosition();
        final Position scaledPosition = scalePosition(startingPosition, factorX, factorY);
        positionable.setPosition(scaledPosition);
    }

    private static Position scalePosition(Position position, double factorX, double factorY) {
        return new Position(position.getX() * factorX,
                position.getY() * factorY);
    }

}
