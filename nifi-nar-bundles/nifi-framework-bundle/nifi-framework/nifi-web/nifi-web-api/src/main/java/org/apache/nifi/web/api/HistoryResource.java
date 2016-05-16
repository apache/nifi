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

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import com.wordnik.swagger.annotations.Authorization;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.action.ActionDTO;
import org.apache.nifi.web.api.dto.action.HistoryDTO;
import org.apache.nifi.web.api.dto.action.HistoryQueryDTO;
import org.apache.nifi.web.api.entity.ActionEntity;
import org.apache.nifi.web.api.entity.ComponentHistoryEntity;
import org.apache.nifi.web.api.entity.HistoryEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.DateTimeParameter;
import org.apache.nifi.web.api.request.IntegerParameter;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * RESTful endpoint for querying the history of this Controller.
 */
@Path("/history")
@Api(
    value = "/history",
    description = "Endpoint for accessing flow history."
)
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
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("") // necessary due to bug in swagger
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets configuration history",
            response = HistoryEntity.class,
            authorizations = {
                @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response queryHistory(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The offset into the result set.",
                    required = true
            )
            @QueryParam("offset") IntegerParameter offset,
            @ApiParam(
                    value = "The number of actions to return.",
                    required = true
            )
            @QueryParam("count") IntegerParameter count,
            @ApiParam(
                    value = "The field to sort on.",
                    required = false
            )
            @QueryParam("sortColumn") String sortColumn,
            @ApiParam(
                    value = "The direction to sort.",
                    required = false
            )
            @QueryParam("sortOrder") String sortOrder,
            @ApiParam(
                    value = "Include actions after this date.",
                    required = false
            )
            @QueryParam("startDate") DateTimeParameter startDate,
            @ApiParam(
                    value = "Include actions before this date.",
                    required = false
            )
            @QueryParam("endDate") DateTimeParameter endDate,
            @ApiParam(
                    value = "Include actions performed by this user.",
                    required = false
            )
            @QueryParam("userName") String userName,
            @ApiParam(
                    value = "Include actions on this component.",
                    required = false
            )
            @QueryParam("sourceId") String sourceId) {

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
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @Path("{id}")
    @ApiOperation(
            value = "Gets an action",
            response = ActionEntity.class,
            authorizations = {
                @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 404, message = "The specified resource could not be found."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getAction(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The action id.",
                    required = true
            )
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
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("") // necessary due to bug in swagger
    // TODO - @PreAuthorize("hasRole('ROLE_ADMIN')")
    @ApiOperation(
            value = "Purges history",
            response = HistoryEntity.class,
            authorizations = {
                @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response deleteHistory(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "Purge actions before this date/time.",
                    required = true
            )
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
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("processors/{processorId}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets configuration history for a processor",
            response = ComponentHistoryEntity.class,
            authorizations = {
                @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 404, message = "The specified resource could not be found."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getProcessorHistory(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The processor id.",
                    required = true
            )
            @PathParam("processorId") final String processorId) {

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ComponentHistoryEntity entity = new ComponentHistoryEntity();
        entity.setComponentHistory(serviceFacade.getComponentHistory(processorId));

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the actions for the specified controller service.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param controllerServiceId The id of the controller service.
     * @return An componentHistoryEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("controller-services/{controllerServiceId}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets configuration history for a controller service",
            response = ComponentHistoryEntity.class,
            authorizations = {
                @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 404, message = "The specified resource could not be found."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getControllerServiceHistory(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The controller service id.",
                    required = true
            )
            @PathParam("controllerServiceId") final String controllerServiceId) {

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ComponentHistoryEntity entity = new ComponentHistoryEntity();
        entity.setComponentHistory(serviceFacade.getComponentHistory(controllerServiceId));

        // generate the response
        return generateOkResponse(entity).build();
    }

    /**
     * Gets the actions for the specified reporting task.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param reportingTaskId The id of the reporting task.
     * @return An componentHistoryEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("reporting-tasks/{reportingTaskId}")
    // TODO - @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets configuration history for a reporting task",
            response = ComponentHistoryEntity.class,
            authorizations = {
                @Authorization(value = "Read Only", type = "ROLE_MONITOR"),
                @Authorization(value = "Data Flow Manager", type = "ROLE_DFM"),
                @Authorization(value = "Administrator", type = "ROLE_ADMIN")
            }
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 404, message = "The specified resource could not be found."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getReportingTaskHistory(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "The reporting task id.",
                    required = true
            )
            @PathParam("reportingTaskId") final String reportingTaskId) {

        // create the revision
        final RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        final ComponentHistoryEntity entity = new ComponentHistoryEntity();
        entity.setComponentHistory(serviceFacade.getComponentHistory(reportingTaskId));

        // generate the response
        return generateOkResponse(entity).build();
    }

    /* setters */
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

}
