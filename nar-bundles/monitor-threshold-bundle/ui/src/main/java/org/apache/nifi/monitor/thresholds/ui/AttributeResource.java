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
package org.apache.nifi.monitor.thresholds.ui;
/*
 * NOTE: rule is synonymous with threshold
 */

import java.util.UUID;
import javax.net.ssl.SSLEngineResult.Status;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.settings.generated.ObjectFactory;
import org.apache.nifi.settings.generated.Rule;
import org.apache.nifi.settings.generated.Attribute;

/**
 * MonitorThreshold REST Web Service (for handling changes to attributes made by
 * the user in the advanced ui)
 *
 */
@Path("/settings/processor/{procid}")
public class AttributeResource extends ThresholdSettingsResource {

    @Context
    private HttpServletRequest request;

    @Context
    private ServletContext servletContext;

    public AttributeResource() {
        super();
        //logger.debug("Invoking AttributeResource.java AttributeResource() constructor.");
    }

    /**
     * Returns a list of all attributes
     *
     * @param processorid
     * @param sortcolumn
     * @param sord
     * @param attributefilter
     * @param rowNum
     * @param pageCommand
     * @return
     */
    @GET
    @Path("/attributes")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_XML)
    public Response getAllAttributes(@PathParam(PROCID) String processorid,
            @QueryParam("sidx") @DefaultValue(ATTRIBUTENAME) String sortcolumn,
            @QueryParam("sord") @DefaultValue("asc") String sord,
            @QueryParam("attributefilter") @DefaultValue("") String attributefilter,
            @QueryParam("rownum") @DefaultValue("-1") String rowNum,
            @QueryParam("pagecom") @DefaultValue("") String pageCommand) {

        //logger.debug("Invoking AttributeResource.java @GET @PATH(\"/attributes\"): getAllAttributes(...)");
        Integer rownum = Integer.parseInt(rowNum);

        return generateOkResponse(getConfigFile(request, servletContext).getConfigAttributes(sortcolumn, getIsAsc(sord), attributefilter, rownum, pageCommand)).build();
    }

    @GET
    @Path("/attributeInfo")
    @Produces(MediaType.WILDCARD)
    public Response getAllAttributes(@PathParam(PROCID) String processorid) {

        //logger.debug("Invoking AttributeResource.java @GET @PATH(\"/attributeInfo\"): getAllAttributes(...)");
        return generateOkResponse(getConfigFile(request, servletContext).getListStats()).build();

    }

    /**
     * Inserts/adds a new attribute.
     *
     * @param processorid
     * @param attributeName
     * @param size
     * @param count
     * @return
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_XML)
    @Path("/attribute")
    public Response createAttribute(@PathParam(PROCID) String processorid,
            @FormParam(ATTRIBUTENAME) @DefaultValue(StringUtils.EMPTY) String attributeName,
            @FormParam(SIZE) @DefaultValue(StringUtils.EMPTY) String size,
            @FormParam(COUNT) @DefaultValue(StringUtils.EMPTY) String count) {

        //logger.debug("Invoking AttributeResource.java @PUT @PATH(\"/attribute\"): createAttribute(" + attributeName + ", " + size + ", " + count + ")");
        try {
            if (attributeName.isEmpty()) {
                validation_error_list.add(INVALID_ATTRIBUTE_NAME);
            }
            if (!validateStringAsLong(size)) {
                validation_error_list.add(INVALID_SIZE);
            }
            if (!validateStringAsInt(count)) {
                validation_error_list.add(INVALID_COUNT);
            }
            ThresholdsConfigFile thresholds = getConfigFile(request, servletContext);
            Attribute tg = thresholds.findAttributebyAttributeName(attributeName);
            if (tg != null) {
                validation_error_list.add(DUPLICATE_ATTRIBUTE_NAME + " " + attributeName);
            }

            if (!validation_error_list.isEmpty()) {
                return Response.status(400).entity(setValidationErrorMessage(validation_error_list)).build();
            }

            ObjectFactory of = new ObjectFactory();
            //create new attribute
            Attribute attr = of.createAttribute();
            attr.setAttributeName(attributeName);
            attr.setId(UUID.randomUUID().toString());

            //create default rule and add to attribute rules
            Rule rule = of.createRule();
            rule.setValue("Default");
            rule.setCount(getBigIntValueOf(count));
            rule.setSize(getBigIntValueOf(size));
            attr.setNoMatchRule(rule);

            thresholds.getFlowFileAttributes().add(attr);

            //Save values to config file
            thresholds.save();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return Response.status(400).entity(GENERAL_ERROR + ex.getMessage()).build();
        }

        return generateOkResponse(Status.OK.toString()).build();
    }

    /**
     * Updates an attribute.
     *
     * @param processorid
     * @param uuid
     * @param attributeName
     * @return
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_XML)
    @Path("/attribute")
    public Response updateAttribute(@PathParam(PROCID) String processorid,
            @FormParam(ATTRIBUTE_UUID) String uuid,
            @FormParam(ATTRIBUTENAME) @DefaultValue(StringUtils.EMPTY) String attributeName) {

        //logger.debug("Invoking AttributeResource.java @POST @PATH(\"/attribute\"): updateAttribute(" + attributeName + ")");
        try {
            if (attributeName.isEmpty()) {
                validation_error_list.add(INVALID_ATTRIBUTE_NAME);
            }
            ThresholdsConfigFile config = getConfigFile(request, servletContext);
            Attribute tg = config.findAttributebyAttributeName(attributeName);
            if (tg != null) {
                if (tg.getId().compareTo(uuid) != 0) {
                    validation_error_list.add(DUPLICATE_ATTRIBUTE_NAME + " " + attributeName);
                }
            }
            if (!validation_error_list.isEmpty()) {
                return Response.status(400).entity(setValidationErrorMessage(validation_error_list)).build();
            }

            Attribute attr = config.findAttribute(uuid);
            attr.setAttributeName(attributeName);

            //Save values to config file
            config.save();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return Response.status(400).entity(GENERAL_ERROR + ex.getMessage()).build();
        }
        return generateOkResponse(Status.OK.toString()).build();
    }

    /**
     * Deletes an attribute.
     *
     * @param processorid
     * @param uuid
     * @return
     */
    @DELETE
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_XML)
    @Path("/attribute")
    public Response deleteAttribute(@PathParam(PROCID) String processorid,
            @FormParam(ID) String uuid) {

        //logger.debug("Invoking AttributeResource.java @DELETE @PATH(\"/attribute\"): deleteAttribute(...)");
        try {
            ThresholdsConfigFile config = getConfigFile(request, servletContext);

            Attribute attr = config.findAttribute(uuid);
            if (attr != null) {
                config.getFlowFileAttributes().remove(attr);

                //Save values to config file
                config.save();
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return Response.status(400).entity(GENERAL_ERROR + ex.getMessage()).build();
        }
        return generateOkResponse(Status.OK.toString()).build();
    }

}
