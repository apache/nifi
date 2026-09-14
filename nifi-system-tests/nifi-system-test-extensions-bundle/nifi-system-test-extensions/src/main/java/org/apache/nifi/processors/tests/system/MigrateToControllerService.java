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

package org.apache.nifi.processors.tests.system;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Set;

/**
 * Pre-upgrade shape of a processor that keeps its store location in a plain property.
 * The post-upgrade shape of the same processor, in the alternate-config extensions bundle,
 * migrates that property into a Controller Service.
 */
@InputRequirement(Requirement.INPUT_FORBIDDEN)
public class MigrateToControllerService extends AbstractProcessor {

    static final PropertyDescriptor STORE_NAME = new PropertyDescriptor.Builder()
            .name("store-name")
            .displayName("Store Name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(STORE_NAME);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        context.yield();
    }

}
