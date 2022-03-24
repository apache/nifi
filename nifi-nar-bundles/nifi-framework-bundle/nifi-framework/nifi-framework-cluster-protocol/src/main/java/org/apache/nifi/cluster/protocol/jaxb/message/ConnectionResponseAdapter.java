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
import org.apache.nifi.cluster.protocol.ConnectionResponse;

/**
 */
public class ConnectionResponseAdapter extends XmlAdapter<AdaptedConnectionResponse, ConnectionResponse> {

    @Override
    public AdaptedConnectionResponse marshal(final ConnectionResponse cr) {
        final AdaptedConnectionResponse aCr = new AdaptedConnectionResponse();
        if (cr != null) {
            aCr.setDataFlow(cr.getDataFlow());
            aCr.setNodeIdentifier(cr.getNodeIdentifier());
            aCr.setTryLaterSeconds(cr.getTryLaterSeconds());
            aCr.setRejectionReason(cr.getRejectionReason());
            aCr.setInstanceId(cr.getInstanceId());
            aCr.setNodeConnectionStatuses(cr.getNodeConnectionStatuses());
            aCr.setComponentRevisions(cr.getComponentRevisions());
        }
        return aCr;
    }

    @Override
    public ConnectionResponse unmarshal(final AdaptedConnectionResponse aCr) {
        if (aCr.shouldTryLater()) {
            return new ConnectionResponse(aCr.getTryLaterSeconds(), aCr.getRejectionReason());
        } else if (aCr.getRejectionReason() != null) {
            return ConnectionResponse.createRejectionResponse(aCr.getRejectionReason());
        } else {
            return new ConnectionResponse(aCr.getNodeIdentifier(), aCr.getDataFlow(),
                aCr.getInstanceId(), aCr.getNodeConnectionStatuses(), aCr.getComponentRevisions());
        }
    }

}
