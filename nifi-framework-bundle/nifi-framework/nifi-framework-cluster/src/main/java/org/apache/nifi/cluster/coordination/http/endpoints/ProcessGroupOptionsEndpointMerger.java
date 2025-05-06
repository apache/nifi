package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.web.api.entity.ProcessGroupOptionEntity;
import org.apache.nifi.web.api.entity.ProcessGroupOptionsEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class ProcessGroupOptionsEndpointMerger implements EndpointResponseMerger {
    public static final Pattern CONTROLLER_SERVICES_MOVE_OPTIONS_URI = Pattern.compile("/nifi-api/controller-services/(?:(?:root)|(?:[a-f0-9\\-]{36}))/move-options");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && CONTROLLER_SERVICES_MOVE_OPTIONS_URI.matcher(uri.getPath()).matches();
    }

    @Override
    public final NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final ProcessGroupOptionsEntity responseEntity = clientResponse.getClientResponse().readEntity(ProcessGroupOptionsEntity.class);
        final Set<ProcessGroupOptionEntity> processGroupOptionEntities = new HashSet<>(responseEntity.getProcessGroupOptionEntities());

        for (final NodeResponse nodeResponse : successfulResponses) {
            final ProcessGroupOptionsEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().readEntity(ProcessGroupOptionsEntity.class);
            final List<ProcessGroupOptionEntity> nodeProcessGroupOptionEntities = nodeResponseEntity.getProcessGroupOptionEntities();

            processGroupOptionEntities.addAll(nodeProcessGroupOptionEntities);
        }

        responseEntity.setProcessGroupOptionEntities(new ArrayList<>(processGroupOptionEntities));

        // create a new client response
        return new NodeResponse(clientResponse, responseEntity);
    }
}
