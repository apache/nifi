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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.client.ClientCache;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ignite.AbstractIgniteProcessor;
import org.apache.nifi.processors.ignite.ClientType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Base class of Ignite cache based processor.
 */
public abstract class AbstractIgniteCacheProcessor extends AbstractIgniteProcessor {

    /**
     * Ignite cache name.
     */
    static final PropertyDescriptor CACHE_NAME = new PropertyDescriptor.Builder()
            .displayName("Ignite Cache Name")
            .name("ignite-cache-name")
            .description("The name of the Ignite cache.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * The Ignite cache key attribute.
     */
    static final PropertyDescriptor IGNITE_CACHE_ENTRY_KEY = new PropertyDescriptor.Builder()
            .displayName("Ignite Cache Entry Identifier")
            .name("ignite-cache-entry-identifier")
            .description("A FlowFile attribute, or attribute expression used " +
                    "for determining Ignite cache key for the FlowFile content.")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    /**
     * Relationships.
     */
    private static Set<Relationship> relationships;

    static {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    /**
     * Ignite cache name.
     */
    private String cacheName;

    /**
     * Get the thick Ignite client cache instance.
     */
    IgniteCache<String, byte[]> getThickIgniteClientCache() {
        if (getThickIgniteClient() == null)
            return null;
        else {
            return getThickIgniteClient().getOrCreateCache(cacheName);
        }
    }

    /**
     * Get the thin Ignite client cache instance.
     */
    ClientCache<String, byte[]> getThinIgniteClientCache() {
        if (getThinIgniteClient() == null)
            return null;
        else {
            return getThinIgniteClient().getOrCreateCache(cacheName);
        }
    }

    /**
     * Get the relationships.
     *
     * @return Relationships.
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * Initialize the Ignite cache instance.
     *
     * @param context Process context.
     * @throws ProcessException If there is a problem while scheduling the processor.
     */
    void initializeIgniteCache(final ProcessContext context) throws ProcessException {
        getLogger().info("Initializing Ignite cache.");
        final ClientType clientType = ClientType.valueOf(context.getProperty(IGNITE_CLIENT_TYPE).getValue());
        try {
            if ((clientType.equals(ClientType.THICK) && getThickIgniteClient() == null)
                    || (clientType.equals(ClientType.THIN) && getThinIgniteClient() == null)) {
                getLogger().info("Initializing Ignite as client.");
                initializeIgniteClient(context);
            }
            cacheName = context.getProperty(CACHE_NAME).getValue();
        } catch (final Exception exception) {
            getLogger().error("Failed to initialize Ignite cache due to {}.", new Object[]{exception}, exception);
            throw new ProcessException(exception);
        }
    }

    /**
     * Close Ignite client cache instance and calls base class closeIgniteClient.
     */
    @OnStopped
    @OnDisabled
    @OnShutdown
    public void closeIgniteClientCache() {
        if (getThickIgniteClientCache() != null) {
            getLogger().info("Closing thick Ignite client cache.");
            getThickIgniteClientCache().close();
        }
        closeIgniteClient();
    }
}
