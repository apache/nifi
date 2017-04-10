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

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.StandardDataFlow;

/**
 */
public class DataFlowAdapter extends XmlAdapter<AdaptedDataFlow, DataFlow> {

    @Override
    public AdaptedDataFlow marshal(final DataFlow df) {
        final AdaptedDataFlow aDf = new AdaptedDataFlow();

        if (df != null) {
            aDf.setFlow(df.getFlow());
            aDf.setSnippets(df.getSnippets());
            aDf.setAuthorizerFingerprint(df.getAuthorizerFingerprint());
            aDf.setMissingComponents(df.getMissingComponents());
        }

        return aDf;
    }

    @Override
    public DataFlow unmarshal(final AdaptedDataFlow aDf) {
        final StandardDataFlow dataFlow = new StandardDataFlow(aDf.getFlow(), aDf.getSnippets(), aDf.getAuthorizerFingerprint(), aDf.getMissingComponents());
        return dataFlow;
    }

}
