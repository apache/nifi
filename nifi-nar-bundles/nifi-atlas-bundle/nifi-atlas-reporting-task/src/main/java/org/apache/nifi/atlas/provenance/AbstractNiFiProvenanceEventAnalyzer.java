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
package org.apache.nifi.atlas.provenance;

import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.nifi.provenance.ProvenanceEventType;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;

public abstract class AbstractNiFiProvenanceEventAnalyzer implements NiFiProvenanceEventAnalyzer {

    /**
     * Utility method to parse a string uri silently.
     * @param uri uri to parse
     * @return parsed URI instance
     */
    protected URI parseUri(String uri) {
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            final String msg = String.format("Failed to parse uri %s due to %s", uri, e);
            throw new IllegalArgumentException(msg, e);
        }
    }

    /**
     * Utility method to parse a string uri silently.
     * @param url url to parse
     * @return parsed URL instance
     */
    protected URL parseUrl(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            final String msg = String.format("Failed to parse url %s due to %s", url, e);
            throw new IllegalArgumentException(msg, e);
        }
    }

    protected DataSetRefs singleDataSetRef(String componentId, ProvenanceEventType eventType, Referenceable ref) {
        final DataSetRefs refs = new DataSetRefs(componentId);
        switch (eventType) {
            case SEND:
            case REMOTE_INVOCATION:
                refs.addOutput(ref);
                break;
            case FETCH:
            case RECEIVE:
                refs.addInput(ref);
                break;
        }

        return refs;
    }

    /**
     * Utility method to split comma separated host names. Port number will be removed.
     */
    protected String[] splitHostNames(String hostNames) {
        return Arrays.stream(hostNames.split(","))
                .map(hostName -> hostName.split(":")[0].trim())
                .toArray(String[]::new);
    }

}
