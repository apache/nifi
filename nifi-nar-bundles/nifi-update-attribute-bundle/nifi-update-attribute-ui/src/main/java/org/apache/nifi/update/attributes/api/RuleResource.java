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
package org.apache.nifi.update.attributes.api;

import java.text.Collator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.update.attributes.Action;
import org.apache.nifi.update.attributes.Condition;
import org.apache.nifi.update.attributes.Criteria;
import org.apache.nifi.update.attributes.Rule;
import org.apache.nifi.update.attributes.UpdateAttributeModelFactory;
import org.apache.nifi.update.attributes.dto.DtoFactory;
import org.apache.nifi.update.attributes.dto.RuleDTO;
import org.apache.nifi.update.attributes.entity.ActionEntity;
import org.apache.nifi.update.attributes.entity.ConditionEntity;
import org.apache.nifi.update.attributes.entity.RuleEntity;
import org.apache.nifi.update.attributes.entity.RulesEntity;
import org.apache.nifi.update.attributes.serde.CriteriaSerDe;
import org.apache.nifi.web.InvalidRevisionException;
import org.apache.nifi.web.Revision;
import org.apache.commons.lang3.StringUtils;

import com.sun.jersey.api.NotFoundException;

import org.apache.nifi.update.attributes.FlowFilePolicy;
import org.apache.nifi.update.attributes.entity.EvaluationContextEntity;
import org.apache.nifi.web.ComponentDetails;
import org.apache.nifi.web.HttpServletConfigurationRequestContext;
import org.apache.nifi.web.HttpServletRequestContext;
import org.apache.nifi.web.NiFiWebConfigurationContext;
import org.apache.nifi.web.NiFiWebConfigurationRequestContext;
import org.apache.nifi.web.NiFiWebRequestContext;
import org.apache.nifi.web.UiExtensionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@Path("/criteria")
public class RuleResource {

    private static final Logger logger = LoggerFactory.getLogger(RuleResource.class);

    @Context
    private ServletContext servletContext;

    @Context
    private HttpServletRequest request;

    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/evaluation-context")
    public Response getEvaluationContext(@QueryParam("processorId") final String processorId) {

        // get the web context
        final NiFiWebConfigurationContext nifiWebContext = (NiFiWebConfigurationContext) servletContext.getAttribute("nifi-web-configuration-context");

        // build the web context config
        final NiFiWebRequestContext contextConfig = getRequestContext(processorId);

        // load the criteria
        final Criteria criteria = getCriteria(nifiWebContext, contextConfig);

        // create the response entity
        final EvaluationContextEntity responseEntity = new EvaluationContextEntity();
        responseEntity.setProcessorId(processorId);
        responseEntity.setFlowFilePolicy(criteria.getFlowFilePolicy().name());
        responseEntity.setRuleOrder(criteria.getRuleOrder());

        // generate the response
        final ResponseBuilder response = Response.ok(responseEntity);
        return noCache(response).build();
    }

    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/evaluation-context")
    public Response updateEvaluationContext(
            @Context final UriInfo uriInfo,
            final EvaluationContextEntity requestEntity) {

        // get the web context
        final NiFiWebConfigurationContext configurationContext = (NiFiWebConfigurationContext) servletContext.getAttribute("nifi-web-configuration-context");

        // ensure the evaluation context has been specified
        if (requestEntity == null) {
            throw new WebApplicationException(badRequest("The evaluation context must be specified."));
        }

        // ensure the id has been specified
        if (requestEntity.getRuleOrder() == null && requestEntity.getFlowFilePolicy() == null) {
            throw new WebApplicationException(badRequest("Either the rule order or the matching strategy must be specified."));
        }

        // build the web context config
        final NiFiWebConfigurationRequestContext requestContext = getConfigurationRequestContext(
                requestEntity.getProcessorId(), requestEntity.getRevision(), requestEntity.getClientId());

        // load the criteria
        final Criteria criteria = getCriteria(configurationContext, requestContext);

        // if a new rule order is specified, attempt to set it
        if (requestEntity.getRuleOrder() != null) {
            try {
                criteria.reorder(requestEntity.getRuleOrder());
            } catch (final IllegalArgumentException iae) {
                throw new WebApplicationException(iae, badRequest(iae.getMessage()));
            }
        }

        // if a new matching strategy is specified, attempt to set it
        if (requestEntity.getFlowFilePolicy() != null) {
            try {
                criteria.setFlowFilePolicy(FlowFilePolicy.valueOf(requestEntity.getFlowFilePolicy()));
            } catch (final IllegalArgumentException iae) {
                throw new WebApplicationException(iae, badRequest("The specified matching strategy is unknown: " + requestEntity.getFlowFilePolicy()));
            }
        }

        // save the criteria
        saveCriteria(requestContext, criteria);

        // create the response entity
        final EvaluationContextEntity responseEntity = new EvaluationContextEntity();
        responseEntity.setClientId(requestEntity.getClientId());
        responseEntity.setRevision(requestEntity.getRevision());
        responseEntity.setProcessorId(requestEntity.getProcessorId());
        responseEntity.setFlowFilePolicy(criteria.getFlowFilePolicy().name());
        responseEntity.setRuleOrder(criteria.getRuleOrder());

        // generate the response
        final ResponseBuilder response = Response.ok(responseEntity);
        return noCache(response).build();
    }

    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/rules")
    public Response createRule(
            @Context final UriInfo uriInfo,
            final RuleEntity requestEntity) {

        // get the web context
        final NiFiWebConfigurationContext configurationContext = (NiFiWebConfigurationContext) servletContext.getAttribute("nifi-web-configuration-context");
        final VariableRegistry variableRegistry = configurationContext.getVariableRegistry();

        // ensure the rule has been specified
        if (requestEntity == null || requestEntity.getRule() == null) {
            throw new WebApplicationException(badRequest("The rule must be specified."));
        }

        final RuleDTO ruleDto = requestEntity.getRule();

        // ensure the id hasn't been specified
        if (ruleDto.getId() != null) {
            throw new WebApplicationException(badRequest("The rule id cannot be specified."));
        }

        // ensure there are some conditions
        if (ruleDto.getConditions() == null || ruleDto.getConditions().isEmpty()) {
            throw new WebApplicationException(badRequest("The rule conditions must be set."));
        }

        // ensure there are some actions
        if (ruleDto.getActions() == null || ruleDto.getActions().isEmpty()) {
            throw new WebApplicationException(badRequest("The rule actions must be set."));
        }

        // generate a new id
        final String uuid = UUID.randomUUID().toString();

        // build the request context
        final NiFiWebConfigurationRequestContext requestContext = getConfigurationRequestContext(
                requestEntity.getProcessorId(), requestEntity.getRevision(), requestEntity.getClientId());

        // load the criteria
        final Criteria criteria = getCriteria(configurationContext, requestContext);
        final UpdateAttributeModelFactory factory = new UpdateAttributeModelFactory(variableRegistry);

        // create the new rule
        final Rule rule;
        try {
            rule = factory.createRule(ruleDto);
            rule.setId(uuid);
        } catch (final IllegalArgumentException iae) {
            throw new WebApplicationException(iae, badRequest(iae.getMessage()));
        }

        // add the rule
        criteria.addRule(rule);

        // save the criteria
        saveCriteria(requestContext, criteria);

        // create the response entity
        final RuleEntity responseEntity = new RuleEntity();
        responseEntity.setClientId(requestEntity.getClientId());
        responseEntity.setRevision(requestEntity.getRevision());
        responseEntity.setProcessorId(requestEntity.getProcessorId());
        responseEntity.setRule(DtoFactory.createRuleDTO(rule));

        // generate the response
        final UriBuilder uriBuilder = uriInfo.getAbsolutePathBuilder();
        final ResponseBuilder response = Response.created(uriBuilder.path(uuid).build()).entity(responseEntity);
        return noCache(response).build();
    }

    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/rules/conditions")
    public Response createCondition(
            @Context final UriInfo uriInfo,
            @PathParam("id") final String ruleId,
            final ConditionEntity requestEntity) {

        // generate a new id
        final String uuid = UUID.randomUUID().toString();

        // get the variable registry
        final NiFiWebConfigurationContext configurationContext = (NiFiWebConfigurationContext) servletContext.getAttribute("nifi-web-configuration-context");
        final VariableRegistry variableRegistry = configurationContext.getVariableRegistry();

        final Condition condition;
        try {
            // create the condition object
            final UpdateAttributeModelFactory factory = new UpdateAttributeModelFactory(variableRegistry);
            condition = factory.createCondition(requestEntity.getCondition());
            condition.setId(uuid);
        } catch (final IllegalArgumentException iae) {
            throw new WebApplicationException(iae, badRequest(iae.getMessage()));
        }

        // build the response
        final ConditionEntity responseEntity = new ConditionEntity();
        responseEntity.setClientId(requestEntity.getClientId());
        responseEntity.setProcessorId(requestEntity.getProcessorId());
        responseEntity.setRevision(requestEntity.getRevision());
        responseEntity.setCondition(DtoFactory.createConditionDTO(condition));

        // generate the response
        final UriBuilder uriBuilder = uriInfo.getAbsolutePathBuilder();
        final ResponseBuilder response = Response.created(uriBuilder.path(uuid).build()).entity(responseEntity);
        return noCache(response).build();
    }

    @POST
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/rules/actions")
    public Response createAction(
            @Context final UriInfo uriInfo,
            @PathParam("id") final String ruleId,
            final ActionEntity requestEntity) {

        // generate a new id
        final String uuid = UUID.randomUUID().toString();

        // get the variable registry
        final NiFiWebConfigurationContext configurationContext = (NiFiWebConfigurationContext) servletContext.getAttribute("nifi-web-configuration-context");
        final VariableRegistry variableRegistry = configurationContext.getVariableRegistry();

        final Action action;
        try {
            // create the condition object
            final UpdateAttributeModelFactory factory = new UpdateAttributeModelFactory(variableRegistry);
            action = factory.createAction(requestEntity.getAction());
            action.setId(uuid);
        } catch (final IllegalArgumentException iae) {
            throw new WebApplicationException(iae, badRequest(iae.getMessage()));
        }

        // build the response
        final ActionEntity responseEntity = new ActionEntity();
        responseEntity.setClientId(requestEntity.getClientId());
        responseEntity.setProcessorId(requestEntity.getProcessorId());
        responseEntity.setRevision(requestEntity.getRevision());
        responseEntity.setAction(DtoFactory.createActionDTO(action));

        // generate the response
        final UriBuilder uriBuilder = uriInfo.getAbsolutePathBuilder();
        final ResponseBuilder response = Response.created(uriBuilder.path(uuid).build()).entity(responseEntity);
        return noCache(response).build();
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/rules/{id}")
    public Response getRule(
            @PathParam("id") final String ruleId,
            @QueryParam("processorId") final String processorId,
            @DefaultValue("false") @QueryParam("verbose") final Boolean verbose) {

        // get the web context
        final NiFiWebConfigurationContext configurationContext = (NiFiWebConfigurationContext) servletContext.getAttribute("nifi-web-configuration-context");

        // build the web context config
        final NiFiWebRequestContext requestContext = getRequestContext(processorId);

        // load the criteria and get the rule
        final Criteria criteria = getCriteria(configurationContext, requestContext);
        final Rule rule = criteria.getRule(ruleId);

        if (rule == null) {
            throw new NotFoundException();
        }

        // convert to a dto
        final RuleDTO ruleDto = DtoFactory.createRuleDTO(rule);

        // prune if appropriate
        if (!verbose) {
            ruleDto.setConditions(null);
            ruleDto.setActions(null);
        }

        // create the response entity
        final RuleEntity responseEntity = new RuleEntity();
        responseEntity.setProcessorId(processorId);
        responseEntity.setRule(ruleDto);

        // generate the response
        final ResponseBuilder response = Response.ok(responseEntity);
        return noCache(response).build();
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/rules")
    public Response getRules(
            @QueryParam("processorId") final String processorId,
            @DefaultValue("false") @QueryParam("verbose") final Boolean verbose) {

        // get the web context
        final NiFiWebConfigurationContext configurationContext = (NiFiWebConfigurationContext) servletContext.getAttribute("nifi-web-configuration-context");

        // build the web context config
        final NiFiWebRequestContext requestContext = getRequestContext(processorId);

        // load the criteria
        final Criteria criteria = getCriteria(configurationContext, requestContext);
        final List<Rule> rules = criteria.getRules();

        // generate the rules
        List<RuleDTO> ruleDtos = null;
        if (rules != null) {
            ruleDtos = new ArrayList<>(rules.size());
            for (final Rule rule : rules) {
                final RuleDTO ruleDto = DtoFactory.createRuleDTO(rule);
                ruleDtos.add(ruleDto);

                // prune if appropriate
                if (!verbose) {
                    ruleDto.setConditions(null);
                    ruleDto.setActions(null);
                }
            }
        }

        // create the response entity
        final RulesEntity responseEntity = new RulesEntity();
        responseEntity.setProcessorId(processorId);
        responseEntity.setRules(ruleDtos);

        // generate the response
        final ResponseBuilder response = Response.ok(responseEntity);
        return noCache(response).build();
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/rules/search-results")
    public Response searchRules(
            @QueryParam("processorId") final String processorId,
            @QueryParam("q") final String term) {

        // get the web context
        final NiFiWebConfigurationContext configurationContext = (NiFiWebConfigurationContext) servletContext.getAttribute("nifi-web-configuration-context");

        // build the web context config
        final NiFiWebRequestContext requestContext = getRequestContext(processorId);

        // load the criteria
        final Criteria criteria = getCriteria(configurationContext, requestContext);
        final List<Rule> rules = criteria.getRules();

        // generate the rules
        List<RuleDTO> ruleDtos = null;
        if (rules != null) {
            ruleDtos = new ArrayList<>(rules.size());
            for (final Rule rule : rules) {
                if (StringUtils.containsIgnoreCase(rule.getName(), term)) {
                    final RuleDTO ruleDto = DtoFactory.createRuleDTO(rule);
                    ruleDtos.add(ruleDto);
                }
            }
        }

        // sort the rules
        Collections.sort(ruleDtos, new Comparator<RuleDTO>() {
            @Override
            public int compare(RuleDTO r1, RuleDTO r2) {
                final Collator collator = Collator.getInstance(Locale.US);
                return collator.compare(r1.getName(), r2.getName());
            }
        });

        // create the response entity
        final RulesEntity responseEntity = new RulesEntity();
        responseEntity.setProcessorId(processorId);
        responseEntity.setRules(ruleDtos);

        // generate the response
        final ResponseBuilder response = Response.ok(responseEntity);
        return noCache(response).build();
    }

    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/rules/{id}")
    public Response updateRule(
            @Context final UriInfo uriInfo,
            @PathParam("id") final String ruleId,
            final RuleEntity requestEntity) {

        // get the web context
        final NiFiWebConfigurationContext nifiWebContext = (NiFiWebConfigurationContext) servletContext.getAttribute("nifi-web-configuration-context");
        // get the variable registry
        final VariableRegistry variableRegistry = nifiWebContext.getVariableRegistry();

        // ensure the rule has been specified
        if (requestEntity == null || requestEntity.getRule() == null) {
            throw new WebApplicationException(badRequest("The rule must be specified."));
        }

        final RuleDTO ruleDto = requestEntity.getRule();

        // ensure the id has been specified
        if (ruleDto.getId() == null) {
            throw new WebApplicationException(badRequest("The rule id must be specified."));
        }
        if (!ruleDto.getId().equals(ruleId)) {
            throw new WebApplicationException(badRequest("The rule id in the path does not equal the rule id in the request body."));
        }

        // ensure the rule name was specified
        if (ruleDto.getName() == null || ruleDto.getName().isEmpty()) {
            throw new WebApplicationException(badRequest("The rule name must be specified and cannot be blank."));
        }

        // ensure there are some conditions
        if (ruleDto.getConditions() == null || ruleDto.getConditions().isEmpty()) {
            throw new WebApplicationException(badRequest("The rule conditions must be set."));
        }

        // ensure there are some actions
        if (ruleDto.getActions() == null || ruleDto.getActions().isEmpty()) {
            throw new WebApplicationException(badRequest("The rule actions must be set."));
        }

        // build the web context config
        final NiFiWebConfigurationRequestContext requestContext = getConfigurationRequestContext(
                requestEntity.getProcessorId(), requestEntity.getRevision(), requestEntity.getClientId());

        // load the criteria
        final UpdateAttributeModelFactory factory = new UpdateAttributeModelFactory(variableRegistry);
        final Criteria criteria = getCriteria(nifiWebContext, requestContext);

        // attempt to locate the rule
        Rule rule = criteria.getRule(ruleId);

        // if the rule isn't found add it
        boolean newRule = false;
        if (rule == null) {
            newRule = true;

            rule = new Rule();
            rule.setId(ruleId);
        }

        try {
            // evaluate the conditions and actions before modifying the rule
            final Set<Condition> conditions = factory.createConditions(ruleDto.getConditions());
            final Set<Action> actions = factory.createActions(ruleDto.getActions());

            // update the rule
            rule.setName(ruleDto.getName());
            rule.setConditions(conditions);
            rule.setActions(actions);
        } catch (final IllegalArgumentException iae) {
            throw new WebApplicationException(iae, badRequest(iae.getMessage()));
        }

        // add the new rule if application
        if (newRule) {
            criteria.addRule(rule);
        }

        // save the criteria
        saveCriteria(requestContext, criteria);

        // create the response entity
        final RuleEntity responseEntity = new RuleEntity();
        responseEntity.setClientId(requestEntity.getClientId());
        responseEntity.setRevision(requestEntity.getRevision());
        responseEntity.setProcessorId(requestEntity.getProcessorId());
        responseEntity.setRule(DtoFactory.createRuleDTO(rule));

        // generate the response
        final ResponseBuilder response;
        if (newRule) {
            final UriBuilder uriBuilder = uriInfo.getAbsolutePathBuilder();
            response = Response.created(uriBuilder.path(ruleId).build()).entity(responseEntity);
        } else {
            response = Response.ok(responseEntity);
        }
        return noCache(response).build();
    }

    @DELETE
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/rules/{id}")
    public Response deleteRule(
            @PathParam("id") final String ruleId,
            @QueryParam("processorId") final String processorId,
            @QueryParam("clientId") final String clientId,
            @QueryParam("revision") final Long revision) {

        // get the web context
        final NiFiWebConfigurationContext configurationContext = (NiFiWebConfigurationContext) servletContext.getAttribute("nifi-web-configuration-context");

        // build the web context config
        final NiFiWebConfigurationRequestContext requestContext = getConfigurationRequestContext(processorId, revision, clientId);

        // load the criteria and get the rule
        final Criteria criteria = getCriteria(configurationContext, requestContext);
        final Rule rule = criteria.getRule(ruleId);

        if (rule == null) {
            throw new NotFoundException();
        }

        // delete the rule
        criteria.deleteRule(rule);

        // save the criteria
        saveCriteria(requestContext, criteria);

        // create the response entity
        final RulesEntity responseEntity = new RulesEntity();
        responseEntity.setClientId(clientId);
        responseEntity.setRevision(revision);
        responseEntity.setProcessorId(processorId);

        // generate the response
        final ResponseBuilder response = Response.ok(responseEntity);
        return noCache(response).build();
    }

    private Criteria getCriteria(final NiFiWebConfigurationContext configurationContext, final NiFiWebRequestContext requestContext) {
        final ComponentDetails processorDetails;

        try {
            // load the processor configuration
            processorDetails = configurationContext.getComponentDetails(requestContext);
        } catch (final InvalidRevisionException ire) {
            throw new WebApplicationException(ire, invalidRevision(ire.getMessage()));
        } catch (final Exception e) {
            final String message = String.format("Unable to get UpdateAttribute[id=%s] criteria: %s", requestContext.getId(), e);
            logger.error(message, e);
            throw new WebApplicationException(e, error(message));
        }

        Criteria criteria = null;
        if (processorDetails != null) {
            try {
                criteria = CriteriaSerDe.deserialize(processorDetails.getAnnotationData());
            } catch (final IllegalArgumentException iae) {
                final String message = String.format("Unable to deserialize existing rules for UpdateAttribute[id=%s]. Deserialization error: %s", requestContext.getId(), iae);
                logger.error(message, iae);
                throw new WebApplicationException(iae, error(message));
            }
        }
        // ensure the criteria isn't null
        if (criteria == null) {
            criteria = new Criteria();
        }

        return criteria;
    }

    private void saveCriteria(final NiFiWebConfigurationRequestContext requestContext, final Criteria criteria) {
        // serialize the criteria
        final String annotationData = CriteriaSerDe.serialize(criteria);

        // get the web context
        final NiFiWebConfigurationContext configurationContext = (NiFiWebConfigurationContext) servletContext.getAttribute("nifi-web-configuration-context");

        try {
            // save the annotation data
            configurationContext.updateComponent(requestContext, annotationData, null);
        } catch (final InvalidRevisionException ire) {
            throw new WebApplicationException(ire, invalidRevision(ire.getMessage()));
        } catch (final Exception e) {
            final String message = String.format("Unable to save UpdateAttribute[id=%s] criteria: %s", requestContext.getId(), e);
            logger.error(message, e);
            throw new WebApplicationException(e, error(message));
        }
    }

    private NiFiWebRequestContext getRequestContext(final String processorId) {
        return new HttpServletRequestContext(UiExtensionType.ProcessorConfiguration, request) {
            @Override
            public String getId() {
                return processorId;
            }
        };
    }

    private NiFiWebConfigurationRequestContext getConfigurationRequestContext(final String processorId, final Long revision, final String clientId) {
        return new HttpServletConfigurationRequestContext(UiExtensionType.ProcessorConfiguration, request) {
            @Override
            public String getId() {
                return processorId;
            }

            @Override
            public Revision getRevision() {
                return new Revision(revision, clientId, processorId);
            }
        };
    }

    private Response badRequest(final String message) {
        return Response.status(Response.Status.BAD_REQUEST).entity(message).type("text/plain").build();
    }

    private Response invalidRevision(final String message) {
        return Response.status(Response.Status.CONFLICT).entity(message).type("text/plain").build();
    }

    private Response error(final String message) {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(message).type("text/plain").build();
    }

    private ResponseBuilder noCache(ResponseBuilder response) {
        CacheControl cacheControl = new CacheControl();
        cacheControl.setPrivate(true);
        cacheControl.setNoCache(true);
        cacheControl.setNoStore(true);
        return response.cacheControl(cacheControl);
    }
}
