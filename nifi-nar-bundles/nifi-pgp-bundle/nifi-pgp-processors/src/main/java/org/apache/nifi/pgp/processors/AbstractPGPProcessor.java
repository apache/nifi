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
package org.apache.nifi.pgp.processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.pgp.controllerservices.PGPService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * Common base class for our various PGP processors.
 *
 * This class provides a helper for creating a {@link PropertyDescriptor} for a {@link PGPService}.  The processors
 * use this helper to make the property in a consistent manner.  Each processor supplies a different description to
 * help the user understand the way the controller is used by the processor.
 *
 * The set of flow relationships is the same for all four processors, so this class provides the relationship
 * members and a helper method for creating their set.
 *
 */
public abstract class AbstractPGPProcessor extends AbstractProcessor {
    public static final String SERVICE_ID = "pgp-service";
    public static final String SERVICE_NAME = "PGP Controller Service";
    public static final String DEFAULT_SIGNATURE_ATTRIBUTE = "content-signature";

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully processed by a PGP operation will be routed to success")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be processed by a PGP operation will be routed to failure")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    /**
     * Helper for subclasses to create their own PGP controller service property.
     *
     * @param desc {@link PropertyDescriptor} description
     * @return new required {@link PropertyDescriptor} with constant name and given description
     */
    static PropertyDescriptor buildControllerServiceProperty(String desc) {
        return new PropertyDescriptor.Builder()
                .name(SERVICE_ID)
                .displayName(SERVICE_NAME)
                .description(desc)
                .required(true)
                .identifiesControllerService(PGPService.class)
                .build();
    }

    /**
     * Returns the relationships for a PGP processor, one success and one failure.
     *
     * @return {@link Set} of processor {@link Relationship}.
     */
    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }
}
