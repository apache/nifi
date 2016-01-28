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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@EventDriven
@SupportsBatching
@Tags({"map", "cache", "fetch", "distributed"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Computes a cache key from FlowFile attributes, for each incoming FlowFile, and fetches the value from the Distributed Map Cache associated "
        + "with that key. The incoming FlowFile's content is replaced with the binary data received by the Distributed Map Cache. If there is no value stored "
        + "under that key then the flow file will be routed to 'not-found'. Note that the processor will always attempt to read the entire cached value into "
        + "memory before placing it in it's destination. This could be potentially problematic if the cached value is very large.")
@WritesAttribute(attribute = "user-defined", description = "If the 'Put Cache Value In Attribute' property is set then whatever it is set to "
        + "will become the attribute key and the value would be whatever the response was from the Distributed Map Cache.")
@SeeAlso(classNames = {"org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService", "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "org.apache.nifi.processors.standard.PutDistributedMapCache"})
public class FetchDistributedMapCache extends AbstractProcessor {

    public static final PropertyDescriptor PROP_DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("Distributed Cache Service")
            .description("The Controller Service that is used to get the cached values.")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    public static final PropertyDescriptor PROP_CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("Cache Entry Identifier")
            .description("A FlowFile attribute, or the results of an Attribute Expression Language statement, which will be evaluated "
                    + "against a FlowFile in order to determine the value used to identify duplicates; it is this value that is cached")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .defaultValue("${hash.value}")
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Put Cache Value In Attribute")
            .description("If set, the cache value received will be put into an attribute of the FlowFile instead of a the content of the"
                    + "FlowFile. The attribute key to put to is determined by evaluating value of this property.")
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor PROP_PUT_ATTRIBUTE_MAX_LENGTH = new PropertyDescriptor.Builder()
            .name("Max Length To Put In Attribute")
            .description("If routing the cache value to an attribute of the FlowFile (by setting the \"Put Cache Value in attribute\" "
                    + "property), the number of characters put to the attribute value will be at most this amount. This is important because "
                    + "attributes are held in memory and large attributes will quickly cause out of memory issues. If the output goes "
                    + "longer than this value, it will be truncated to fit. Consider making this smaller if able.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("256")
            .build();

    public static final PropertyDescriptor PROP_CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the cached value is encoded. This will only be used when routing to an attribute.")
            .required(false)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("If the cache was successfully communicated with it will be routed to this relationship")
            .build();
    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not-found")
            .description("If a FlowFile's Cache Entry Identifier was not found in the cache, it will be routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If unable to communicate with the cache or if the cache entry is evaluated to be blank, the FlowFile will be penalized and routed to this relationship")
            .build();
    private final Set<Relationship> relationships;

    private final Serializer<String> keySerializer = new StringSerializer();
    private final Deserializer<byte[]> valueDeserializer = new CacheValueDeserializer();

    public FetchDistributedMapCache() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_NOT_FOUND);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROP_CACHE_ENTRY_IDENTIFIER);
        descriptors.add(PROP_DISTRIBUTED_CACHE_SERVICE);
        descriptors.add(PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE);
        descriptors.add(PROP_PUT_ATTRIBUTE_MAX_LENGTH);
        descriptors.add(PROP_CHARACTER_SET);
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

        final ProcessorLog logger = getLogger();
        final String cacheKey = context.getProperty(PROP_CACHE_ENTRY_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isBlank(cacheKey)) {
            logger.error("FlowFile {} has no attribute for given Cache Entry Identifier", new Object[]{flowFile});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        final DistributedMapCacheClient cache = context.getProperty(PROP_DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

        try {
            final byte[] cacheValue = cache.get(cacheKey, keySerializer, valueDeserializer);

            if(cacheValue==null){
                session.transfer(flowFile, REL_NOT_FOUND);
                logger.info("Could not find an entry in cache for {}; routing to not-found", new Object[]{flowFile});

            } else {
                boolean putInAttribute = context.getProperty(PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE).isSet();
                if(putInAttribute){
                    String attributeName = context.getProperty(PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
                    String attributeValue = new String(cacheValue,context.getProperty(PROP_CHARACTER_SET).getValue());

                    int maxLength = context.getProperty(PROP_PUT_ATTRIBUTE_MAX_LENGTH).asInteger();
                    if(maxLength < attributeValue.length()){
                        attributeValue = attributeValue.substring(0,maxLength);
                    }

                    flowFile = session.putAttribute(flowFile, attributeName, attributeValue);

                } else {
                    flowFile = session.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(OutputStream out) throws IOException {
                            out.write(cacheValue);
                        }
                    });
                }

                session.transfer(flowFile, REL_SUCCESS);
                if(putInAttribute){
                    logger.info("Found a cache key of {} and added an attribute to {} with it's value.", new Object[]{cacheKey, flowFile});
                }else {
                    logger.info("Found a cache key of {} and replaced the contents of {} with it's value.", new Object[]{cacheKey, flowFile});
                }
            }

        } catch (final IOException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with cache when processing {} due to {}", new Object[]{flowFile, e});
        }
    }


    public static class CacheValueDeserializer implements Deserializer<byte[]> {

        @Override
        public byte[] deserialize(final byte[] input) throws DeserializationException, IOException {
            if (input == null || input.length == 0) {
                return null;
            }
            return input;
        }
    }

    public static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

}
