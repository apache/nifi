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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.cs.tests.system.SetStateService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.Set;

public class SetState extends AbstractProcessor {

    static final PropertyDescriptor STATE_SERVICE = new Builder()
        .name("State Service")
        .identifiesControllerService(SetStateService.class)
        .required(true)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(STATE_SERVICE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final SetStateService service = context.getProperty(STATE_SERVICE).asControllerService(SetStateService.class);
        try {
            service.setState("local", "local", Scope.LOCAL);
            service.setState("cluster", "cluster", Scope.CLUSTER);
        } catch (final Exception e) {
            throw new ProcessException(e);
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
