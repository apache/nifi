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
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinQueryDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.BulletinBoardEntity;
import org.apache.nifi.web.api.request.BulletinBoardPatternParameter;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.apache.nifi.web.api.request.IntegerParameter;
import org.apache.nifi.web.api.request.LongParameter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for managing a Template.
 */
@Api(hidden = true)
public class BulletinBoardResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(BulletinBoardResource.class);

    private NiFiServiceFacade serviceFacade;

    /**
     * Retrieves all the of templates in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a
     * new one will be generated. This value (whether specified or generated) is
     * included in the response.
     * @param after Supporting querying for bulletins after a particular
     * bulletin id.
     * @param limit The max number of bulletins to return.
     * @param sourceName Source name filter. Supports a regular expression.
     * @param message Message filter. Supports a regular expression.
     * @param sourceId Source id filter. Supports a regular expression.
     * @param groupId Group id filter. Supports a regular expression.
     * @return A bulletinBoardEntity.
     */
    @GET
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("") // necessary due to bug in swagger
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @ApiOperation(
            value = "Gets current bulletins",
            response = BulletinBoardEntity.class,
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
    public Response getBulletinBoard(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @ApiParam(
                    value = "Includes bulletins with an id after this value.",
                    required = false
            )
            @QueryParam("after") LongParameter after,
            @ApiParam(
                    value = "Includes bulletins originating from this sources whose name match this regular expression.",
                    required = false
            )
            @QueryParam("sourceName") BulletinBoardPatternParameter sourceName,
            @ApiParam(
                    value = "Includes bulletins whose message that match this regular expression.",
                    required = false
            )
            @QueryParam("message") BulletinBoardPatternParameter message,
            @ApiParam(
                    value = "Includes bulletins originating from this sources whose id match this regular expression.",
                    required = false
            )
            @QueryParam("sourceId") BulletinBoardPatternParameter sourceId,
            @ApiParam(
                    value = "Includes bulletins originating from this sources whose group id match this regular expression.",
                    required = false
            )
            @QueryParam("groupId") BulletinBoardPatternParameter groupId,
            @ApiParam(
                    value = "The number of bulletins to limit the response to.",
                    required = false
            )
            @QueryParam("limit") IntegerParameter limit) {

        // build the bulletin query
        final BulletinQueryDTO query = new BulletinQueryDTO();

        if (sourceId != null) {
            query.setSourceId(sourceId.getRawPattern());
        }
        if (groupId != null) {
            query.setGroupId(groupId.getRawPattern());
        }
        if (sourceName != null) {
            query.setName(sourceName.getRawPattern());
        }
        if (message != null) {
            query.setMessage(message.getRawPattern());
        }
        if (after != null) {
            query.setAfter(after.getLong());
        }
        if (limit != null) {
            query.setLimit(limit.getInteger());
        }

        // get the bulletin board
        final BulletinBoardDTO bulletinBoard = serviceFacade.getBulletinBoard(query);

        // create the revision
        RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // create the response entity
        BulletinBoardEntity entity = new BulletinBoardEntity();
        entity.setRevision(revision);
        entity.setBulletinBoard(bulletinBoard);

        // generate the response
        return clusterContext(generateOkResponse(entity)).build();
    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

}
