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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.FlowFileAttributesSerializer;

@EventDriven
@SupportsBatching
@Tags({"map", "cache", "wait", "hold", "distributed", "signal", "release"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Routes incoming FlowFiles to the 'wait' relationship until a matching release signal "
        + "is stored in the distributed cache from a corresponding Notify processor.  At this point, a waiting FlowFile is routed to "
        + "the 'success' relationship, with attributes copied from the FlowFile that produced "
        + "the release signal from the Notify processor.  The release signal entry is then removed from "
        + "the cache.  Waiting FlowFiles will be routed to 'expired' if they exceed the Expiration Duration.")
@WritesAttribute(attribute = "wait.start.timestamp", description = "All FlowFiles will have an attribute 'wait.start.timestamp', which sets the "
        + "initial epoch timestamp when the file first entered this processor.  This is used to determine the expiration time of the FlowFile.")
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService", "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "org.apache.nifi.processors.standard.Notify"})
public class Wait extends AbstractProcessor {

    public static final String WAIT_START_TIMESTAMP = "wait.start.timestamp";

    // Identifies the distributed map cache client
    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
        .name("Distributed Cache Service")
        .description("The Controller Service that is used to check for release signals from a corresponding Notify processor")
        .required(true)
        .identifiesControllerService(DistributedMapCacheClient.class)
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

    // Selects the FlowFile attribute or expression, whose value is used as cache key
    public static final PropertyDescriptor EXPIRATION_DURATION = new PropertyDescriptor.Builder()
        .name("Expiration Duration")
        .description("Indicates the duration after which waiting flow files will be routed to the 'expired' relationship")
        .required(true)
        .defaultValue("10 min")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(false)
        .build();

    public static final AllowableValue ATTRIBUTE_COPY_REPLACE = new AllowableValue("replace", "Replace if present",
            "When cached attributes are copied onto released FlowFiles, they replace any matching attributes.");

    public static final AllowableValue ATTRIBUTE_COPY_KEEP_ORIGINAL = new AllowableValue("keeporiginal", "Keep original",
            "Attributes on released FlowFiles are not overwritten by copied cached attributes.");

    public static final PropertyDescriptor ATTRIBUTE_COPY_MODE = new PropertyDescriptor.Builder()
        .name("Attribute Copy Mode")
        .description("Specifies how to handle attributes copied from flow files entering the Notify processor")
        .defaultValue(ATTRIBUTE_COPY_KEEP_ORIGINAL.getValue())
        .required(true)
        .allowableValues(ATTRIBUTE_COPY_REPLACE, ATTRIBUTE_COPY_KEEP_ORIGINAL)
        .expressionLanguageSupported(false)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("A FlowFile with a matching release signal in the cache will be routed to this relationship")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("When the cache cannot be reached, or if the Release Signal Identifier evaluates to null or empty, FlowFiles will be routed to this relationship")
        .build();

    public static final Relationship REL_WAIT = new Relationship.Builder()
        .name("wait")
        .description("A FlowFile with no matching release signal in the cache will be routed to this relationship")
        .build();

    public static final Relationship REL_EXPIRED = new Relationship.Builder()
        .name("expired")
        .description("A FlowFile that has exceeded the configured Expiration Duration will be routed to this relationship")
        .build();
    private final Set<Relationship> relationships;

    private final Serializer<String> keySerializer = new StringSerializer();
    private final Deserializer<Map<String, String>> valueDeserializer = new FlowFileAttributesSerializer();

    public Wait() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_WAIT);
        rels.add(REL_EXPIRED);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(RELEASE_SIGNAL_IDENTIFIER);
        descriptors.add(EXPIRATION_DURATION);
        descriptors.add(DISTRIBUTED_CACHE_SERVICE);
        descriptors.add(ATTRIBUTE_COPY_MODE);
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

        // cache key is computed from attribute 'RELEASE_SIGNAL_IDENTIFIER' with expression language support
        final String cacheKey = context.getProperty(RELEASE_SIGNAL_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();

        // if the computed value is null, or empty, we transfer the flow file to failure relationship
        if (StringUtils.isBlank(cacheKey)) {
            logger.error("FlowFile {} has no attribute for given Release Signal Identifier", new Object[] {flowFile});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // the cache client used to interact with the distributed cache
        final DistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

        try {
            // check for expiration
            String waitStartTimestamp = flowFile.getAttribute(WAIT_START_TIMESTAMP);
            if (waitStartTimestamp == null) {
                waitStartTimestamp = String.valueOf(System.currentTimeMillis());
                flowFile = session.putAttribute(flowFile, WAIT_START_TIMESTAMP, waitStartTimestamp);
            }

            long lWaitStartTimestamp = 0L;
            try {
                lWaitStartTimestamp = Long.parseLong(waitStartTimestamp);
            } catch (NumberFormatException nfe) {
                logger.error("{} has an invalid value '{}' on FlowFile {}", new Object[] {WAIT_START_TIMESTAMP, waitStartTimestamp, flowFile});
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
            long expirationDuration = context.getProperty(EXPIRATION_DURATION)
                    .asTimePeriod(TimeUnit.MILLISECONDS);
            long now = System.currentTimeMillis();
            if (now > (lWaitStartTimestamp + expirationDuration)) {
                logger.warn("FlowFile {} expired after {}ms", new Object[] {flowFile, (now - lWaitStartTimestamp)});
                session.transfer(flowFile, REL_EXPIRED);
                return;
            }

            // get notifying flow file attributes
            Map<String, String> cachedAttributes = cache.get(cacheKey, keySerializer, valueDeserializer);

            if (cachedAttributes == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("No release signal yet for {} on FlowFile {}", new Object[] {cacheKey, flowFile});
                }
                session.transfer(flowFile, REL_WAIT);
                return;
            }

            // copy over attributes from release signal flow file, if provided
            if (!cachedAttributes.isEmpty()) {
                cachedAttributes.remove("uuid");
                String attributeCopyMode = context.getProperty(ATTRIBUTE_COPY_MODE).getValue();
                if (ATTRIBUTE_COPY_REPLACE.getValue().equals(attributeCopyMode)) {
                    flowFile = session.putAllAttributes(flowFile, cachedAttributes);
                } else {
                    Map<String, String> attributesToCopy = new HashMap<>();
                    for(Entry<String, String> entry : cachedAttributes.entrySet()) {
                        // if the current flow file does *not* have the cached attribute, copy it
                        if (flowFile.getAttribute(entry.getKey()) == null) {
                            attributesToCopy.put(entry.getKey(), entry.getValue());
                        }
                    }
                    flowFile = session.putAllAttributes(flowFile, attributesToCopy);
                }
            }

            session.transfer(flowFile, REL_SUCCESS);

            cache.remove(cacheKey, keySerializer);
        } catch (final IOException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with cache when processing {} due to {}", new Object[] {flowFile, e});
        }
    }

    /**
     * Simple string serializer, used for serializing the cache key
     */
    public static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

}
