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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.ignite.IgniteCache;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ignite.AbstractIgniteProcessor;

/**
 * Base class of Ignite cache based processor
 */
public abstract class AbstractIgniteCacheProcessor extends AbstractIgniteProcessor {

    /**
     * Ignite cache name
     */
    protected static final PropertyDescriptor CACHE_NAME = new PropertyDescriptor.Builder()
            .displayName("Ignite Cache Name")
            .name("ignite-cache-name")
            .description("The name of the ignite cache")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * The Ignite cache key attribute
     */
    public static final PropertyDescriptor IGNITE_CACHE_ENTRY_KEY = new PropertyDescriptor.Builder()
            .displayName("Ignite Cache Entry Identifier")
            .name("ignite-cache-entry-identifier")
            .description("A FlowFile attribute, or attribute expression used " +
                "for determining Ignite cache key for the Flow File content")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    /**
     * Relations
     */
    protected static Set<Relationship> relationships;

    /**
     * Ignite cache name
     */
    private String cacheName;

    /**
     * Get ignite cache instance
     * @return ignite cache instance
     */
    protected IgniteCache<String, byte[]> getIgniteCache() {
         if ( getIgnite() == null )
            return null;
         else
            return getIgnite().getOrCreateCache(cacheName);
    }

    static {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * Initialize the ignite cache instance
     * @param context process context
     * @throws ProcessException if there is a problem while scheduling the processor
     */
    public void initializeIgniteCache(ProcessContext context) throws ProcessException {

        getLogger().info("Initializing Ignite cache");

        try {
            if ( getIgnite() == null ) {
                getLogger().info("Initializing ignite as client");
                super.initializeIgnite(context);
            }

            cacheName = context.getProperty(CACHE_NAME).getValue();

        } catch (Exception e) {
            getLogger().error("Failed to initialize ignite cache due to {}", new Object[] { e }, e);
            throw new ProcessException(e);
        }
    }

    /**
     * Close Ignite cache instance and calls base class closeIgnite
     */
    @OnShutdown
    public void closeIgniteCache() {
        if (getIgniteCache() != null) {
            getLogger().info("Closing ignite cache");
            getIgniteCache().close();
        }
        super.closeIgnite();
    }
}
