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
package org.apache.nifi.minifi.c2.util;

import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class LogMarkerUtil {

    public static Marker getMarker(C2Heartbeat heartbeat) {
        Marker marker = MarkerFactory.getMarker("");
        if (heartbeat != null && heartbeat.getAgentId() != null) {
            marker = MarkerFactory.getMarker(heartbeat.getAgentId());
        }

        return marker;
    }

    public static Marker getMarker(C2OperationAck ack) {
        Marker marker = MarkerFactory.getMarker("");
        if (ack.getAgentInfo() != null && ack.getAgentInfo().getIdentifier() != null) {
            marker = MarkerFactory.getMarker(ack.getAgentInfo().getIdentifier());
        }

        return marker;
    }
}
