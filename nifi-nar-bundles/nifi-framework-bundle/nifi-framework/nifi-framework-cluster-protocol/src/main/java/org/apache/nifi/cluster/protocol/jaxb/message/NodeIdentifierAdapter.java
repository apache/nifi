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
import org.apache.nifi.cluster.protocol.NodeIdentifier;

/**
 */
public class NodeIdentifierAdapter extends XmlAdapter<AdaptedNodeIdentifier, NodeIdentifier> {

    @Override
    public AdaptedNodeIdentifier marshal(final NodeIdentifier ni) {
        if (ni == null) {
            return null;
        } else {
            final AdaptedNodeIdentifier aNi = new AdaptedNodeIdentifier();
            aNi.setId(ni.getId());
            aNi.setApiAddress(ni.getApiAddress());
            aNi.setApiPort(ni.getApiPort());
            aNi.setSocketAddress(ni.getSocketAddress());
            aNi.setSocketPort(ni.getSocketPort());
            aNi.setSiteToSiteAddress(ni.getSiteToSiteAddress());
            aNi.setSiteToSitePort(ni.getSiteToSitePort());
            aNi.setSiteToSiteHttpApiPort(ni.getSiteToSiteHttpApiPort());
            aNi.setSiteToSiteSecure(ni.isSiteToSiteSecure());
            return aNi;
        }
    }

    @Override
    public NodeIdentifier unmarshal(final AdaptedNodeIdentifier aNi) {
        if (aNi == null) {
            return null;
        } else {
            return new NodeIdentifier(aNi.getId(), aNi.getApiAddress(), aNi.getApiPort(), aNi.getSocketAddress(), aNi.getSocketPort(),
                aNi.getSiteToSiteAddress(), aNi.getSiteToSitePort(),aNi.getSiteToSiteHttpApiPort(), aNi.isSiteToSiteSecure());
        }
    }

}
