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
package org.apache.nifi.cluster;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class NodeInformationAdapter extends XmlAdapter<AdaptedNodeInformation, NodeInformation> {

    @Override
    public NodeInformation unmarshal(final AdaptedNodeInformation adapted) throws Exception {
        return new NodeInformation(adapted.getHostname(), adapted.getSiteToSitePort(), adapted.getApiPort(), adapted.isSiteToSiteSecure(), adapted.getTotalFlowFiles());
    }

    @Override
    public AdaptedNodeInformation marshal(final NodeInformation nodeInformation) throws Exception {
        final AdaptedNodeInformation adapted = new AdaptedNodeInformation();
        adapted.setHostname(nodeInformation.getHostname());
        adapted.setSiteToSitePort(nodeInformation.getSiteToSitePort());
        adapted.setApiPort(nodeInformation.getAPIPort());
        adapted.setSiteToSiteSecure(nodeInformation.isSiteToSiteSecure());
        adapted.setTotalFlowFiles(nodeInformation.getTotalFlowFiles());
        return adapted;
    }

}
