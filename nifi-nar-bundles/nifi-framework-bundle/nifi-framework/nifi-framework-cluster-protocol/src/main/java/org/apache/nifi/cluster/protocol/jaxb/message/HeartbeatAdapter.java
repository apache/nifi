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
package org.apache.nifi.cluster.protocol.jaxb.message;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import org.apache.nifi.cluster.protocol.Heartbeat;

/**
 */
public class HeartbeatAdapter extends XmlAdapter<AdaptedHeartbeat, Heartbeat> {

    @Override
    public AdaptedHeartbeat marshal(final Heartbeat hb) {
        final AdaptedHeartbeat aHb = new AdaptedHeartbeat();

        if (hb != null) {
            // set node identifier
            aHb.setNodeIdentifier(hb.getNodeIdentifier());

            // set payload
            aHb.setPayload(hb.getPayload());

            // set connected flag
            aHb.setConnectionStatus(hb.getConnectionStatus());
        }

        return aHb;
    }

    @Override
    public Heartbeat unmarshal(final AdaptedHeartbeat aHb) {
        return new Heartbeat(aHb.getNodeIdentifier(), aHb.getConnectionStatus(), aHb.getPayload());
    }

}
