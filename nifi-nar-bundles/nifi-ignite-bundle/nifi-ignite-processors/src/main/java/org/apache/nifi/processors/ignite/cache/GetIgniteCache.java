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
package org.apache.nifi.processors.ignite.cache;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * Get cache processors which gets byte array for the key from Ignite cache and set the array
 * as the FlowFile content.
 */
@EventDriven
@SupportsBatching
@Tags({ "Ignite", "get", "read", "cache", "key" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Get the byte array from Ignite Cache and adds it as the content of a FlowFile." +
    "The processor uses the value of FlowFile attribute (Ignite cache entry key) as the cache key lookup. " +
    "If the entry corresponding to the key is not found in the cache an error message is associated with the FlowFile " +
    "Note - The Ignite Kernel periodically outputs node performance statistics to the logs. This message " +
    " can be turned off by setting the log level for logger 'org.apache.ignite' to WARN in the logback.xml configuration file.")
@WritesAttributes({
    @WritesAttribute(attribute = GetIgniteCache.IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, description = "The reason for getting entry from cache"),
    })
@SeeAlso({PutIgniteCache.class})
public class GetIgniteCache extends AbstractIgniteCacheProcessor {

    /** Flow file attribute keys and messages */
    public static final String IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY = "ignite.cache.get.failed.reason";
    public static final String IGNITE_GET_FAILED_MISSING_KEY_MESSAGE = "The FlowFile key attribute was missing";
    public static final String IGNITE_GET_FAILED_MISSING_ENTRY_MESSAGE = "The cache byte array entry was null or zero length";
    public static final String IGNITE_GET_FAILED_MESSAGE_PREFIX = "The cache request failed because of ";

    /**
     * Property descriptors
     */
    protected static final List<PropertyDescriptor> descriptors =
        Arrays.asList(IGNITE_CONFIGURATION_FILE, CACHE_NAME, IGNITE_CACHE_ENTRY_KEY);

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public final void initialize(ProcessContext context) throws ProcessException {
        super.initializeIgniteCache(context);
    }

    /**
     * Handle flow file and gets the entry from the cache based on the key attribute
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        String key = context.getProperty(IGNITE_CACHE_ENTRY_KEY).evaluateAttributeExpressions(flowFile).getValue();
        if ( StringUtils.isEmpty(key) ) {
            flowFile = session.putAttribute(flowFile, IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY, IGNITE_GET_FAILED_MISSING_KEY_MESSAGE);
            session.transfer(flowFile, REL_FAILURE);
        } else {
            try {
                byte [] value = getIgniteCache().get(key);
                if ( value == null || value.length == 0 ) {
                    flowFile = session.putAttribute(flowFile, IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY,
                        IGNITE_GET_FAILED_MISSING_ENTRY_MESSAGE);
                    session.transfer(flowFile, REL_FAILURE);
                } else {
                    ByteArrayInputStream bais = new ByteArrayInputStream(value);
                    flowFile = session.importFrom(bais, flowFile);
                    session.transfer(flowFile,REL_SUCCESS);
                }
            } catch(Exception exception) {
                flowFile = session.putAttribute(flowFile, IGNITE_GET_FAILED_REASON_ATTRIBUTE_KEY,
                     IGNITE_GET_FAILED_MESSAGE_PREFIX + exception);
                getLogger().error("Failed to get value for key {} from IgniteDB due to {}", new Object[] { key, exception }, exception);
                session.transfer(flowFile, REL_FAILURE);
                context.yield();
            }
        }
    }
}
