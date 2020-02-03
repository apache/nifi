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
package org.apache.nifi.processors.standard.pgp;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import java.util.Set;


/**
 * Common base class for our various PGP processors.
 *
 */
public abstract class AbstractProcessorPGP extends AbstractProcessor {
    public static final String SERVICE_ID = "pgp-key-service";
    public static final String DEFAULT_SIGNATURE_ATTRIBUTE = "content-signature";

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully encrypted or decrypted will be routed to success")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be encrypted or decrypted will be routed to failure")
            .build();

    /**
     * Helper for subclasses to create their own key material controller service property.
     *
     * @param desc Property description
     * @return property for a PGPKeyMaterialService
     */
    static PropertyDescriptor buildKeyServiceProperty(String desc) {
        return new PropertyDescriptor.Builder()
                .name(SERVICE_ID)
                .description(desc)
                .required(true)
                .identifiesControllerService(PGPKeyMaterialService.class)
                .build();
    }

    /**
     * Returns the relationships for a pgp processor, one success and one failure.
     *
     * @return processor relationships
     */
    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS, REL_FAILURE);
    }
}
