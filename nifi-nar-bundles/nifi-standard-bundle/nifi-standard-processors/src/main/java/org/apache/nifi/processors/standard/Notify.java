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
package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@EventDriven
@SupportsBatching
@Tags({"map", "cache", "notify", "distributed", "signal", "release"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Caches a release signal identifier in the distributed cache, optionally along with "
        + "the FlowFile's attributes.  Any flow files held at a corresponding Wait processor will be "
        + "released once this signal in the cache is discovered.")
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService", "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "org.apache.nifi.processors.standard.Wait"})
public class Notify extends AbstractProcessor {

    // Identifies the distributed map cache client
    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
        .name("Distributed Cache Service")
        .description("The Controller Service that is used to cache release signals in order to release files queued at a corresponding Wait processor")
        .required(true)
        .identifiesControllerService(AtomicDistributedMapCacheClient.class)
        .build();

    // Selects the FlowFile attribute or expression, whose value is used as cache key
    public static final PropertyDescriptor RELEASE_SIGNAL_IDENTIFIER = new PropertyDescriptor.Builder()
        .name("Release Signal Identifier")
        .description("A value, or the results of an Attribute Expression Language statement, which will " +
            "be evaluated against a FlowFile in order to determine the release signal cache key")
        .required(true)
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
        .expressionLanguageSupported(true)
        .build();

    public static final PropertyDescriptor SIGNAL_COUNTER_NAME = new PropertyDescriptor.Builder()
        .name("Signal Counter Name")
        .description("A value, or the results of an Attribute Expression Language statement, which will " +
            "be evaluated against a FlowFile in order to determine the signal counter name. " +
            "Signal counter name is useful when a corresponding Wait processor needs to know the number of occurrences " +
            "of different types of events, such as success or failure, or destination data source names, etc.")
        .required(true)
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
        .expressionLanguageSupported(true)
        .defaultValue(WaitNotifyProtocol.DEFAULT_COUNT_NAME)
        .build();

    // Specifies an optional regex used to identify which attributes to cache
    public static final PropertyDescriptor ATTRIBUTE_CACHE_REGEX = new PropertyDescriptor.Builder()
        .name("Attribute Cache Regex")
        .description("Any attributes whose names match this regex will be stored in the distributed cache to be "
                + "copied to any FlowFiles released from a corresponding Wait processor.  Note that the "
                + "uuid attribute will not be cached regardless of this value.  If blank, no attributes "
                + "will be cached.")
        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
        .expressionLanguageSupported(false)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles where the release signal has been successfully entered in the cache will be routed to this relationship")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("When the cache cannot be reached, or if the Release Signal Identifier evaluates to null or empty, FlowFiles will be routed to this relationship")
        .build();

    private final Set<Relationship> relationships;

    public Notify() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(RELEASE_SIGNAL_IDENTIFIER);
        descriptors.add(SIGNAL_COUNTER_NAME);
        descriptors.add(DISTRIBUTED_CACHE_SERVICE);
        descriptors.add(ATTRIBUTE_CACHE_REGEX);
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();

        // Signal id is computed from attribute 'RELEASE_SIGNAL_IDENTIFIER' with expression language support
        final String signalId = context.getProperty(RELEASE_SIGNAL_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();
        final String counterName = context.getProperty(SIGNAL_COUNTER_NAME).evaluateAttributeExpressions(flowFile).getValue();

        // if the computed value is null, or empty, we transfer the flow file to failure relationship
        if (StringUtils.isBlank(signalId)) {
            logger.error("FlowFile {} has no attribute for given Release Signal Identifier", new Object[] {flowFile});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // the cache client used to interact with the distributed cache
        final AtomicDistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(AtomicDistributedMapCacheClient.class);
        final WaitNotifyProtocol protocol = new WaitNotifyProtocol(cache);

        try {
            final String attributeCacheRegex = (context.getProperty(ATTRIBUTE_CACHE_REGEX).isSet())
                    ? context.getProperty(ATTRIBUTE_CACHE_REGEX).getValue()
                    : null;

            Map<String, String> attributesToCache = new HashMap<>();
            if (StringUtils.isNotEmpty(attributeCacheRegex)) {
                attributesToCache = flowFile.getAttributes().entrySet()
                        .stream().filter(e -> (!e.getKey().equals("uuid") && e.getKey().matches(attributeCacheRegex)))
                        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Cached release signal identifier {} counterName {} from FlowFile {}", new Object[] {signalId, counterName, flowFile});
            }

            // In case of ConcurrentModificationException, just throw the exception so that processor can
            // retry after yielding for a while.
            protocol.notify(signalId, counterName, 1, attributesToCache);

            session.transfer(flowFile, REL_SUCCESS);
        } catch (final IOException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with cache when processing {} due to {}", new Object[] {flowFile, e});
        }
    }

}
