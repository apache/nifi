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

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
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
import org.codehaus.enunciate.jaxrs.TypeHint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * RESTful endpoint for managing a Template.
 */
public class BulletinBoardResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(BulletinBoardResource.class);

    private NiFiServiceFacade serviceFacade;

    /**
     * Retrieves all the of templates in this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @param after Supporting querying for bulletins after a particular bulletin id.
     * @param limit The max number of bulletins to return.
     * @param sourceName Source name filter. Supports a regular expression.
     * @param message Message filter. Supports a regular expression.
     * @param sourceId Source id filter. Supports a regular expression.
     * @param groupId Group id filter. Supports a regular expression.
     * @return A bulletinBoardEntity.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasAnyRole('ROLE_MONITOR', 'ROLE_DFM', 'ROLE_ADMIN')")
    @TypeHint(BulletinBoardEntity.class)
    public Response getBulletinBoard(@QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId,
            @QueryParam("after") LongParameter after, @QueryParam("sourceName") BulletinBoardPatternParameter sourceName,
            @QueryParam("message") BulletinBoardPatternParameter message, @QueryParam("sourceId") BulletinBoardPatternParameter sourceId,
            @QueryParam("groupId") BulletinBoardPatternParameter groupId, @QueryParam("limit") IntegerParameter limit) {

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
