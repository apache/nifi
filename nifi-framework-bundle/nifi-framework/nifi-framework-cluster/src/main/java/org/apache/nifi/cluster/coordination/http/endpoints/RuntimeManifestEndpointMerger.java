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
package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.c2.protocol.component.api.Bundle;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;
import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.web.api.entity.RuntimeManifestEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class RuntimeManifestEndpointMerger implements EndpointResponseMerger {

    public static final String RUNTIME_MANIFEST_URI_PATTERN = "/nifi-api/flow/runtime-manifest";

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equalsIgnoreCase(method) && RUNTIME_MANIFEST_URI_PATTERN.equals(uri.getPath());
    }

    @Override
    public NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses,
                              final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {

        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Merge of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final RuntimeManifestEntity responseEntity = clientResponse.getClientResponse().readEntity(RuntimeManifestEntity.class);
        final RuntimeManifest responseManifest = responseEntity.getRuntimeManifest();
        final Set<Bundle> responseBundles = responseManifest.getBundles() == null ? new LinkedHashSet<>() : new LinkedHashSet<>(responseManifest.getBundles());

        for (final NodeResponse nodeResponse : successfulResponses) {
            final RuntimeManifestEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().readEntity(RuntimeManifestEntity.class);
            final RuntimeManifest nodeResponseManifest = nodeResponseEntity.getRuntimeManifest();
            final List<Bundle> nodeResponseBundles = nodeResponseManifest.getBundles() == null ? Collections.emptyList() : nodeResponseManifest.getBundles();
            responseBundles.retainAll(nodeResponseBundles);
        }

        responseManifest.setBundles(new ArrayList<>(responseBundles));
        return new NodeResponse(clientResponse, responseEntity);
    }

}
