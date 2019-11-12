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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.priority.Rule;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.PriorityRuleDTO;
import org.apache.nifi.web.api.entity.PriorityRuleEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/priority-rules")
@Api(value = "/priority-rules", description = "Endpoint for managing priority rules")
public class PriorityRuleResource extends ApplicationResource {
    private static Logger LOGGER = LoggerFactory.getLogger(PriorityRuleResource.class);
    private static final Gson GSON = new GsonBuilder().create();

    private final NiFiServiceFacade serviceFacade;
    private final Authorizer authorizer;
    private final FlowController flowController;

    public PriorityRuleResource(NiFiServiceFacade serviceFacade, Authorizer authorizer, FlowController flowController, NiFiProperties properties, RequestReplicator requestReplicator,
                                ClusterCoordinator clusterCoordinator) {
        this.serviceFacade = serviceFacade;
        this.authorizer = authorizer;
        this.flowController = flowController;
        setProperties(properties);
        setRequestReplicator(requestReplicator);
        setClusterCoordinator(clusterCoordinator);
        setFlowController(flowController);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("getRuleList")
    @ApiOperation(value = "Retrieves the current rule list", notes = NON_GUARANTEED_ENDPOINT, response = String.class)
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getRuleList() {
        // authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable accessPolicy = lookup.getPriorityRules();
            accessPolicy.authorize(authorizer, RequestAction.READ, NiFiUserUtils.getNiFiUser());
        });

        return generateOkResponse(getRuleListJSON()).build();
    }

    private String getRuleListJSON() {
        List<Rule> ruleList = flowController.getRulesManager().getActiveRuleList();
        JsonObject priorityRules = new JsonObject();
        JsonArray jsonArray = new JsonArray();
        priorityRules.add("priorityRules", jsonArray);

        ruleList.stream().filter(rule -> (rule != Rule.DEFAULT && rule != Rule.UNEVALUATED)).forEach(rule -> {
            JsonObject jsonObjectRule = new JsonObject();
            jsonArray.add(jsonObjectRule);
            jsonObjectRule.addProperty("id", rule.getUuid());
            jsonObjectRule.addProperty("label", rule.getLabel());
            jsonObjectRule.addProperty("expression", rule.getExpression());
            jsonObjectRule.addProperty("rateOfThreadUsage", rule.getRateOfThreadUsage());
        });
        return GSON.toJson(priorityRules);
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("addRule")
    @ApiOperation(value = "Add a new rule to the existing list", notes = NON_GUARANTEED_ENDPOINT)
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response addRule(@ApiParam(value = "the rule to add", required = true)PriorityRuleEntity priorityRuleEntity) {
        LOGGER.debug("addRule({})", priorityRuleEntity);

        // Authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable accessPolicy = lookup.getPriorityRules();
            accessPolicy.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
        });

        PriorityRuleDTO priorityRuleDTO = priorityRuleEntity.getPriorityRule();
        Rule rule = Rule.builder()
                .label(priorityRuleDTO.getLabel())
                .expression(priorityRuleDTO.getExpression())
                .rateOfThreadUsage(priorityRuleDTO.getRateOfThreadUsage())
                .build();

        try {
            flowController.getRulesManager().addOrModifyRule(rule);
            flowController.getRulesManager().writeRules();
            // Set the id on the argument for auditing purposes (See PriorityRulesAuditor)
            priorityRuleDTO.setId(rule.getUuid());

            return generateOkResponse(priorityRuleEntity).status(200).build();
        } catch(Exception e) {
            LOGGER.error("Exception when trying to add rule: {}", priorityRuleEntity, e);
            return generateOkResponse("FAILURE, " + e.getMessage()).build();
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("editRule")
    @ApiOperation(value = "Add a new rule to the existing list", notes = NON_GUARANTEED_ENDPOINT)
    @ApiResponses(
            value = {
                    @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                    @ApiResponse(code = 401, message = "Client could not be authenticated."),
                    @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                    @ApiResponse(code = 404, message = "The specified resource could not be found."),
                    @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response editRule(@ApiParam(value = "the rule to edit", required = true)PriorityRuleEntity priorityRuleEntity) {
        LOGGER.debug("editRule({})", priorityRuleEntity);

        // Authorize access
        serviceFacade.authorizeAccess(lookup -> {
            final Authorizable accessPolicy = lookup.getPriorityRules();
            accessPolicy.authorize(authorizer, RequestAction.WRITE, NiFiUserUtils.getNiFiUser());
        });

        PriorityRuleDTO priorityRuleDTO = priorityRuleEntity.getPriorityRule();
        Rule rule = Rule.builder()
                .uuid(priorityRuleDTO.getId())
                .label(priorityRuleDTO.getLabel())
                .expression(priorityRuleDTO.getExpression())
                .rateOfThreadUsage(priorityRuleDTO.getRateOfThreadUsage())
                .expired(priorityRuleDTO.isExpired())
                .build();

        try {
            flowController.getRulesManager().modifyRule(rule);
            flowController.getRulesManager().writeRules();

            return generateOkResponse(priorityRuleEntity).build();
        } catch(Exception e) {
            LOGGER.error("Exception when trying to edit rule: {}", priorityRuleEntity, e);
            return generateOkResponse("FAILURE, " + e.getMessage()).build();
        }
    }
}
