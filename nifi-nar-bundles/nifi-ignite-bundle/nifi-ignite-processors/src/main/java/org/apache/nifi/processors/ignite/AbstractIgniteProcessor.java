/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.ignite;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * Base class for Ignite processors
 */
public abstract class AbstractIgniteProcessor extends AbstractProcessor  {

    /**
     * Ignite spring configuration file
     */
    public static final PropertyDescriptor IGNITE_CONFIGURATION_FILE = new PropertyDescriptor.Builder()
        .displayName("Ignite Spring Properties Xml File")
        .name("ignite-spring-properties-xml-file")
        .description("Ignite spring configuration file, <path>/<ignite-configuration>.xml. If the " +
            "configuration file is not provided, default Ignite configuration " +
            "configuration is used which binds to 127.0.0.1:47500..47509")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    /**
     * Success relation
     */
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to Ignite cache are routed to this relationship").build();

    /**
     * Failure relation
     */
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to Ignite cache are routed to this relationship").build();

    /**
     * The ignite instance
     */
    private transient Ignite ignite;

    /**
     * Get ignite instance
     * @return ignite instance
     */
    protected Ignite getIgnite() {
        return ignite;
    }

    /**
     * Close ignite instance
     */
    public void closeIgnite() {
        if (ignite != null) {
            getLogger().info("Closing ignite client");
            ignite.close();
            ignite = null;
        }
    }

    /**
     * Initialize ignite instance
     * @param context process context
     */
    public void initializeIgnite(ProcessContext context) {

        if ( getIgnite() != null ) {
            getLogger().info("Ignite already initialized");
            return;
        }


        synchronized(Ignition.class) {
            List<Ignite> grids = Ignition.allGrids();

            if ( grids.size() == 1 ) {
                getLogger().info("Ignite grid already available");
                ignite = grids.get(0);
                return;
            }
            Ignition.setClientMode(true);

            String configuration = context.getProperty(IGNITE_CONFIGURATION_FILE).getValue();
            getLogger().info("Initializing ignite with configuration {} ", new Object[] { configuration });
            if ( StringUtils.isEmpty(configuration) ) {
                ignite = Ignition.start();
            } else {
                ignite = Ignition.start(configuration);
            }
        }
    }
}
