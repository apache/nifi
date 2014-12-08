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

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
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
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.settings.generated.Thresholds;
import org.apache.nifi.settings.generated.Attribute;
import org.apache.nifi.settings.generated.ObjectFactory;
import org.apache.nifi.settings.generated.Rule;

/**
 * MonitorThreshold REST Web Service (for handling changes to thresholds made by
 * the user in the Advanced UI)
 *
 */
@Path("/settings/processor/{procid}/attribute/{attributeuuid}")
public class ThresholdResource extends ThresholdSettingsResource {

    @Context
    private HttpServletRequest request;

    @Context
    private ServletContext servletContext;

    public ThresholdResource() {
        super();
    }

    @GET
    @Path("/rules")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_XML)
    public Response getSubSettings(
            @PathParam(PROCID) String processorid,
            @PathParam(ATTRIBUTE_UUID) String attributeid,
            @QueryParam("sidx") @DefaultValue(ID) String sortcolumn,
            @QueryParam("sord") @DefaultValue("asc") String sord,
            @QueryParam("attributevaluefilter") @DefaultValue("") String attributevaluefilter,
            @QueryParam("sizefilter") @DefaultValue("") String sizefilter,
            @QueryParam("filecountfilter") @DefaultValue("") String filecountfilter) {

        //logger.debug("Invoking ThresholdResource.java @GET @PATH(\"/rules\"): getSubSettings(...)");
        String result = getConfigFile(request, servletContext).getRules(attributeid, sortcolumn, getIsAsc(sord), attributevaluefilter, sizefilter, filecountfilter);
        return generateOkResponse(result).build();
    }

    /**
     * Inserts/adds a new threshold.
     *
     * @param processorid
     * @param attributeid
     * @param value
     * @param size
     * @param count
     * @return
     */
    @PUT
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_XML)
    @Path("/rule")
    public Response ruleInsert(
            @PathParam(PROCID) String processorid,
            @PathParam(ATTRIBUTE_UUID) String attributeid,
            @FormParam(RULEVALUE) @DefaultValue(StringUtils.EMPTY) String value,
            @FormParam(SIZE) @DefaultValue(StringUtils.EMPTY) String size,
            @FormParam(COUNT) @DefaultValue(StringUtils.EMPTY) String count) {

        //logger.debug("Invoking ThresholdResource.java @PUT @PATH(\"/rule\"): ruleInsert(...).  RULEVALUE is: " + value);
        try {
            ThresholdsConfigFile config = getConfigFile(request, servletContext);
            if (value.isEmpty()) {
                validation_error_list.add(INVALID_RULE_ID);
            }
            if (!validateStringAsLong(size)) {
                validation_error_list.add(INVALID_SIZE);
            }
            if (!validateStringAsInt(count)) {
                validation_error_list.add(INVALID_COUNT);
            }
            if (config.containsRule(attributeid, value)) {
                validation_error_list.add(String.format(DUPLICATE_VALUE + "%s", value));
            }
            if (!validation_error_list.isEmpty()) {
                return Response.status(400).entity(setValidationErrorMessage(validation_error_list)).build();
            }

            ObjectFactory of = new ObjectFactory();
            //create new attribute
            Thresholds rule = of.createThresholds();
            rule.setId(value);
            rule.setSize(getBigIntValueOf(size));
            rule.setCount(getBigIntValueOf(count));

            Attribute attr = config.findAttribute(attributeid);
            attr.getRule().add(rule);

            config.save();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return Response.status(400).entity(GENERAL_ERROR + ex.getMessage()).build();
        }
        return generateOkResponse(Status.OK.toString()).build();
    }

    /**
     * Updates a threshold value, size or count.
     *
     * @param processorid
     * @param uuid
     * @param rulevalue
     * @param size
     * @param count
     * @return
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_XML)
    @Path("/rule/{" + RULEVALUE + "}")
    public Response ruleUpdate(@PathParam(PROCID) String processorid,
            @PathParam(ATTRIBUTE_UUID) String uuid,
            //                    @PathParam(RULEUUID) String ruleuuid,
            @PathParam(RULEVALUE) String rulevalue,
            //                    @FormParam(RULEVALUE) @DefaultValue(StringUtils.EMPTY) String rulevalue,
            @FormParam(SIZE) @DefaultValue(StringUtils.EMPTY) String size,
            @FormParam(COUNT) @DefaultValue(StringUtils.EMPTY) String count) {

        //logger.debug("Invoking ThresholdResource.java @POST @PATH(\"/rule/{\"+RULEVALUE+\"}\"): ruleUpdate(...).  RULEVALUE is: " + rulevalue);
        try {
//            if(rulevalue.isEmpty()&& ruleuuid.compareTo("-1")!=0)
//                validation_error_list.add(INVALIDRULEID);
            if (!validateStringAsLong(size)) {
                validation_error_list.add(INVALID_SIZE);
            }
            if (!validateStringAsInt(count)) {
                validation_error_list.add(INVALID_COUNT);
            }
            if (!validation_error_list.isEmpty()) {
                return Response.status(400).entity(setValidationErrorMessage(validation_error_list)).build();
            }

            ThresholdsConfigFile config = getConfigFile(request, servletContext);
            Attribute attr = config.findAttribute(uuid);

//            if(ruleuuid.compareTo("-1")==0){
            if (rulevalue.compareToIgnoreCase("default") == 0) {
                Rule rule = attr.getNoMatchRule();
                rule.setCount(getBigIntValueOf(count));
                rule.setSize(getBigIntValueOf(size));
            } else {
                Thresholds rule = config.findRule(attr, rulevalue);//ruleuuid);
//                 rule.setValue(rulevalue);
                rule.setCount(getBigIntValueOf(count));
                rule.setSize(getBigIntValueOf(size));
            }

            config.save();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return Response.status(400).entity(GENERAL_ERROR + ex.getMessage()).build();
        }
        return generateOkResponse(Status.OK.toString()).build();
    }

    /**
     * Deletes a threshold.
     *
     * @param processorid
     * @param uuid
     * @param rulevalue
     * @return
     */
    @DELETE
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_XML)
    @Path("/rule/{" + ID + "}")
    public Response ruleDelete(@PathParam(PROCID) String processorid,
            @PathParam(ATTRIBUTE_UUID) String uuid,
            @PathParam(ID) String rulevalue) {

        //logger.debug("Invoking ThresholdResource.java @DELETE @PATH(\"/rule/{\"+ID+\"}\"): ruleDelete(...).  RULEVALUE is: " + rulevalue);
        try {
            ThresholdsConfigFile config = getConfigFile(request, servletContext);
            Attribute attr = config.findAttribute(uuid);
            Thresholds rule = config.findRule(attr, rulevalue);//ruleuuid);
            attr.getRule().remove(rule);
            config.save();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            return Response.status(400).entity(GENERAL_ERROR + ex.getMessage()).build();
        }
        return generateOkResponse(Status.OK.toString()).build();
    }
}
