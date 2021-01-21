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
package org.apache.nifi.remote.util;

import java.util.Set;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.nifi.remote.PeerStatus;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;

public class PeerStatusCache {

    private final Set<PeerStatus> statuses;
    private final long timestamp;
    private final String remoteInstanceUris;
    private final SiteToSiteTransportProtocol transportProtocol;

    public PeerStatusCache(final Set<PeerStatus> statuses, final long timestamp,
                           final String remoteInstanceUris,
                           final SiteToSiteTransportProtocol transportProtocol) {
        this.statuses = statuses;
        this.timestamp = timestamp;
        this.remoteInstanceUris = remoteInstanceUris;
        this.transportProtocol = transportProtocol;
    }

    public Set<PeerStatus> getStatuses() {
        return statuses;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String  getRemoteInstanceUris() {
        return remoteInstanceUris;
    }

    public SiteToSiteTransportProtocol getTransportProtocol() {
        return transportProtocol;
    }

    public boolean isEmpty() {
        return statuses == null || statuses.isEmpty();
    }

    @Override
    public String toString() {
        final ToStringBuilder builder = new ToStringBuilder(this);
        ToStringBuilder.setDefaultStyle(ToStringStyle.SHORT_PREFIX_STYLE);
        builder.append("Timestamp", timestamp);
        builder.append("Remote instance URIs", remoteInstanceUris);
        builder.append("Transport protocol", transportProtocol);
        builder.append("Peer status count", statuses != null ? statuses.size() : 0);
        builder.append("Peer statuses", statuses);
        return builder.toString();
    }
}
