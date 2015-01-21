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
package org.apache.nifi.web.api;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.nifi.web.api.entity.ActionEntity;
import org.apache.nifi.web.api.entity.HistoryEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DateTimeParameter;
import org.apache.nifi.web.api.request.IntegerParameter;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.NiFiServiceFacade;
import static org.apache.nifi.web.api.ApplicationResource.CLIENT_ID;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.action.ActionDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.entity.ProcessorHistoryEntity;
import org.codehaus.enunciate.jaxrs.TypeHint;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for querying the history of this Controller.
 */
public class HistoryResource extends ApplicationResource {

    private NiFiServiceFacade serviceFacade;

    /**
     * Queries the history of this Controller.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param offset The offset into the data. This parameter is required and is
     * used in conjunction with count.
     * @param count The number of rows that should be returned. This parameter
     * is required and is used in conjunction with page.
     * @param sortColumn The column to sort on. This parameter is optional. If
     * not specified the results will be returned with the most recent first.
     * @param sortOrder The sort order.
     * @param startDate The start date/time for the query. The start date/time
     * must be formatted as 'MM/dd/yyyy HH:mm:ss'. This parameter is optional
     * and must be specified in the timezone of the server. The server's
     * timezone can be determined by inspecting the result of a status or
     * history request.
     * @param endDate The end date/time for the query. The end date/time must be
     * formatted as 'MM/dd/yyyy HH:mm:ss'. This parameter is optional and must
     * be specified in the timezone of the server. The server's timezone can be
     * determined by inspecting the result of a status or history request.
     * @param userName The user name of the user who's actions are being
     * queried. This parameter is optional.
     * @param sourceId The id of the source being queried (usually a processor
     * id). This parameter is optional.
     * @return A historyEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(HistoryEntity.class)
    public Response queryHistory(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @QueryParam("offset") IntegerParameter offset, @QueryParam("count") IntegerParameter count,
            @QueryParam("sortColumn") String sortColumn, @QueryParam("sortOrder") String sortOrder,
            @QueryParam("startDate") DateTimeParameter startDate, @QueryParam("endDate") DateTimeParameter endDate,
            @QueryParam("userName") String userName, @QueryParam("sourceId") String sourceId) {

        // ensure the page is specified
        if (offset == null) {
            throw new IllegalArgumentException("The desired offset must be specified.");
        } else if (offset.getInteger() < 0) {
            throw new IllegalArgumentException("The desired offset must be an integer value greater than or equal to 0.");
        }

        // ensure the row count is specified
        if (count == null) {
            throw new IllegalArgumentException("The desired row count must be specified.");
        } else if (count.getInteger() < 1) {
            throw new IllegalArgumentException("The desired row count must be an integer value greater than 0.");
        }

        // normalize the sort order
        if (sortOrder != null) {
            if (!sortOrder.equalsIgnoreCase("asc") && !sortOrder.equalsIgnoreCase("desc")) {
                throw new IllegalArgumentException("The sort order must be 'asc' or 'desc'.");
            }
        }

        // ensure the start and end dates are specified
        if (endDate != null && startDate != null) {
            if (endDate.getDateTime().before(startDate.getDateTime())) {
                throw new IllegalArgumentException("The start date/time must come before the end date/time.");
            }
        }

        // create a history query
        final HistoryQueryDTO query = new HistoryQueryDTO();
        query.setSortColumn(sortColumn);
        query.setSortOrder(sortOrder);
        query.setOffset(offset.getInteger());
        query.setCount(count.getInteger());

        // optionally set the start date
        if (startDate != null) {
            query.setStartDate(startDate.getDateTime());
        }

        // optionally set the end date
        if (endDate != null) {
            query.setEndDate(endDate.getDateTime());
        }

        // optionally set the user id
        if (userName != null) {
            query.setUserName(userName);
        }

        // optionally set the processor id
        if (sourceId != null) {
            query.setSourceId(sourceId);
        }

        // perform the query
        final HistoryDTO history = serviceFacade.getActions(query);

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final HistoryEntity entity = new HistoryEntity();
        entity.setRevision(revision);
        entity.setHistory(history);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the action for the corresponding id.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param id The id of the action to get.
     * @return An actionEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @Path("{id}")
    @TypeHint(ActionEntity.class)
    public Response getAction(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("id") IntegerParameter id) {

        // ensure the id was specified
        if (id == null) {
            throw new IllegalArgumentException("The action id must be specified.");
        }

        // get the specified action
        final ActionDTO action = serviceFacade.getAction(id.getInteger());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ActionEntity entity = new ActionEntity();
        entity.setRevision(revision);
        entity.setAction(action);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Deletes flow history from the specified end date.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param endDate The end date for the purge action.
     * @return A historyEntity
     */
    @DELETE
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @TypeHint(HistoryEntity.class)
    public Response deleteHistory(
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @QueryParam("endDate") DateTimeParameter endDate) {

        // ensure the end date is specified
        if (endDate == null) {
            throw new IllegalArgumentException("The end date must be specified.");
        }

        // purge the actions
        serviceFacade.deleteActions(endDate.getDateTime());

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final HistoryEntity entity = new HistoryEntity();
        entity.setRevision(revision);

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the actions for the specified processor.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param processorId The id of the processor.
     * @return An processorHistoryEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @Path("/processors/{processorId}")
    @TypeHint(ProcessorHistoryEntity.class)
    public Response getProcessorHistory(
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @PathParam("processorId") final String processorId) {

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ProcessorHistoryEntity entity = new ProcessorHistoryEntity();
        entity.setRevision(revision);
        entity.setProcessorHistory(serviceFacade.getProcessorHistory(processorId));

        // generate the response
        return generateOkResponse(entity).build();
    }

    /* setters */
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

}
