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
package org.apache.nifi.cluster.protocol;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.cluster.protocol.jaxb.message.DataFlowAdapter;

/**
 * Represents a dataflow, which includes the raw bytes of the flow.xml and
 * whether processors should be started automatically at application startup.
 */
@XmlJavaTypeAdapter(DataFlowAdapter.class)
public class StandardDataFlow implements Serializable, DataFlow {
    private static final long serialVersionUID = 1L;

    private final byte[] flow;
    private final byte[] snippetBytes;
    private final byte[] authorizerFingerprint;
    private final Set<String> missingComponentIds;

    /**
     * Constructs an instance.
     *
     * @param flow a valid flow as bytes, which cannot be null
     * @param snippetBytes an XML representation of snippets.  May be null.
     * @param authorizerFingerprint the bytes of the Authorizer's fingerprint. May be null when using an external Authorizer.
     * @param missingComponentIds the ids of components that were created as missing ghost components
     *
     * @throws NullPointerException if flow is null
     */
    public StandardDataFlow(final byte[] flow, final byte[] snippetBytes, final byte[] authorizerFingerprint, final Set<String> missingComponentIds) {
        if(flow == null){
            throw new NullPointerException("Flow cannot be null");
        }
        this.flow = flow;
        this.snippetBytes = snippetBytes;
        this.authorizerFingerprint = authorizerFingerprint;
        this.missingComponentIds = Collections.unmodifiableSet(missingComponentIds == null
                ? new HashSet<>() : new HashSet<>(missingComponentIds));
    }

    public StandardDataFlow(final DataFlow toCopy) {
        this.flow = copy(toCopy.getFlow());
        this.snippetBytes = copy(toCopy.getSnippets());
        this.authorizerFingerprint = copy(toCopy.getAuthorizerFingerprint());
        this.missingComponentIds = Collections.unmodifiableSet(toCopy.getMissingComponents() == null
                ? new HashSet<>() : new HashSet<>(toCopy.getMissingComponents()));
    }

    private static byte[] copy(final byte[] bytes) {
        return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }

    @Override
    public byte[] getFlow() {
        return flow;
    }


    @Override
    public byte[] getSnippets() {
        return snippetBytes;
    }

    @Override
    public byte[] getAuthorizerFingerprint() {
        return authorizerFingerprint;
    }

    @Override
    public Set<String> getMissingComponents() {
        return missingComponentIds;
    }

}
