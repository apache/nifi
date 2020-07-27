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
package org.apache.nifi.processors.cybersecurity;


import com.idealista.tlsh.TLSH;
import info.debatty.java.spamsum.SpamSum;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Set;

abstract class AbstractFuzzyHashProcessor extends AbstractProcessor {
    final protected static String ssdeep = "ssdeep";
    final protected static String tlsh = "tlsh";

    public static final AllowableValue allowableValueSSDEEP = new AllowableValue(
            ssdeep,
            ssdeep,
            "Uses ssdeep / SpamSum 'context triggered piecewise hash'.");
    public static final AllowableValue allowableValueTLSH = new AllowableValue(
            tlsh,
            tlsh,
            "Uses TLSH (Trend 'Locality Sensitive Hash'). Note: FlowFile Content must be at least 512 characters long");

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("ATTRIBUTE_NAME")
            .displayName("Hash Attribute Name")
            .description("The name of the FlowFile Attribute that should hold the Fuzzy Hash Value")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("fuzzyhash.value")
            .build();

    public static final PropertyDescriptor HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("HASH_ALGORITHM")
            .displayName("Hashing Algorithm")
            .description("The hashing algorithm utilised")
            .allowableValues(allowableValueSSDEEP, allowableValueTLSH)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected List<PropertyDescriptor> descriptors;

    protected Set<Relationship> relationships;

    protected boolean checkMinimumAlgorithmRequirements(String algorithm, FlowFile flowFile) {
        // Check if content matches minimum length requirement
        if (algorithm.equals(tlsh) && flowFile.getSize() < 512 ) {
            return false;
        } else {
            return true;
        }
    }


    protected String generateHash(String algorithm, String content) {
        switch (algorithm) {
            case tlsh:
                return new TLSH(content).hash();
            case ssdeep:
                return new SpamSum().HashString(content);
            default:
                return null;
        }
    }
}
