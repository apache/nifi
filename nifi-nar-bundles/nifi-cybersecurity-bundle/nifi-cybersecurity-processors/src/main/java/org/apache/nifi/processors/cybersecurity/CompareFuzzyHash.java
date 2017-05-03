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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.cybersecurity.matchers.FuzzyHashMatcher;
import org.apache.nifi.processors.cybersecurity.matchers.SSDeepHashMatcher;
import org.apache.nifi.processors.cybersecurity.matchers.TLSHHashMatcher;
import org.apache.nifi.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SeeAlso({FuzzyHashContent.class})
@Tags({"hashing", "fuzzy-hashing", "cyber-security"})
@CapabilityDescription("Compares an attribute containing a Fuzzy Hash against a file containing a list of fuzzy hashes, " +
        "appending an attribute to the FlowFile in case of a successful match.")

@WritesAttributes({
        @WritesAttribute(attribute = "XXXX.N.match", description = "The match that resembles the attribute specified " +
                "by the <Hash Attribute Name> property. Note that: 'XXX' gets replaced with the <Hash Attribute Name>"),
        @WritesAttribute(attribute = "XXXX.N.similarity", description = "The similarity score between this flowfile" +
                "and its match of the same number N. Note that: 'XXX' gets replaced with the <Hash Attribute Name>")})

public class CompareFuzzyHash extends AbstractFuzzyHashProcessor {
    public static final AllowableValue singleMatch = new AllowableValue(
            "single",
            "single",
            "Send FlowFile to matched after the first match above threshold");
    public static final AllowableValue multiMatch = new AllowableValue(
            "multi-match",
            "multi-match",
            "Iterate full list of hashes before deciding to send FlowFile to matched or unmatched");

    public static final PropertyDescriptor HASH_LIST_FILE = new PropertyDescriptor.Builder()
            .name("HASH_LIST_FILE")
            .displayName("Hash List source file")
            .description("Path to the file containing hashes to be validated against")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    // Note we add a PropertyDescriptor HASH_ALGORITHM and ATTRIBUTE_NAME from parent class

    public static final PropertyDescriptor MATCH_THRESHOLD = new PropertyDescriptor.Builder()
            // Note that while both TLSH and SSDeep seems to return int, we treat them as double in code.
            // The rationale behind being the expectation that other algorithms thatmay return double values
            // may be added to the processor later on.
            .name("MATCH_THRESHOLD")
            .displayName("Match threshold")
            .description("The similarity score must exceed or be equal to in order for" +
                    "match to be considered true. Refer to Additional Information for differences between TLSH " +
                    "and SSDEEP scores and how they relate to this property.")
            .required(true)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MATCHING_MODE = new PropertyDescriptor.Builder()
            .name("MATCHING_MODE")
            .displayName("Matching mode")
            .description("Defines if the Processor should try to match as many entries as possible (" + multiMatch.getDisplayName() +
                    ") or if it should stop after the first match (" + singleMatch.getDisplayName() + ")")
            .required(true)
            .allowableValues(singleMatch,multiMatch)
            .defaultValue(singleMatch.getValue())
            .build();

    public static final Relationship REL_FOUND = new Relationship.Builder()
            .name("found")
            .description("Any FlowFile that is successfully matched to an existing hash will be sent to this Relationship.")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not-found")
            .description("Any FlowFile that cannot be matched to an existing hash will be sent to this Relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be matched, e.g. (lacks the attribute) will be sent to this Relationship.")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(HASH_LIST_FILE);
        // As mentioned above, add the PropertyDescriptor HASH_ALGORITHM and ATTRIBUTE_NAME from parent class
        descriptors.add(HASH_ALGORITHM);
        descriptors.add(ATTRIBUTE_NAME);
        descriptors.add(MATCH_THRESHOLD);
        descriptors.add(MATCHING_MODE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_FOUND);
        relationships.add(REL_NOT_FOUND);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        String algorithm = context.getProperty(HASH_ALGORITHM).getValue();

        final String attributeName = context.getProperty(ATTRIBUTE_NAME).getValue();
        String inputHash = flowFile.getAttribute(attributeName);

        if (inputHash == null) {
            getLogger().info("FlowFile {} lacks the required '{}' attribute, routing to failure.",
                    new Object[]{flowFile, attributeName});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        FuzzyHashMatcher fuzzyHashMatcher = null;

        switch (algorithm) {
            case tlsh:
                fuzzyHashMatcher = new TLSHHashMatcher(getLogger());
                break;
            case ssdeep:
                fuzzyHashMatcher = new SSDeepHashMatcher(getLogger());
                break;
            default:
                getLogger().error("Seems like the processor is configured to use unsupported algorithm '{}' ? Yielding.",
                        new Object[]{algorithm});
                context.yield();
                return;
        }

        if (fuzzyHashMatcher.isValidHash(inputHash) == false) {
            // and if that is the case we log
            logger.error("Invalid hash provided. Sending to failure");
            //  and send to failure
            session.transfer(flowFile, REL_FAILURE);
            session.commit();
            return;
        }

        double similarity = 0;
        double matchThreshold = context.getProperty(MATCH_THRESHOLD).asDouble();

        try {
            Map<String, Double> matched = new ConcurrentHashMap<String, Double>();

            BufferedReader reader = fuzzyHashMatcher.getReader(context.getProperty(HASH_LIST_FILE).getValue());

            String line = null;

            iterateFile: while ((line = reader.readLine()) != null) {
                if (line != null) {
                    similarity = fuzzyHashMatcher.getSimilarity(inputHash, line);

                    if (fuzzyHashMatcher.matchExceedsThreshold(similarity, matchThreshold)) {
                        String match = fuzzyHashMatcher.getMatch(line);
                        // A malformed file may cause a match with no filename
                        // Because this would simply look odd, we ignore such entry and log
                        if (!StringUtils.isEmpty(match)) {
                            matched.put(match, similarity);
                        } else {
                            logger.error("Found a match against a malformed entry '{}'. Please inspect the contents of" +
                                    "the {} file and ensure they are properly formatted",
                                    new Object[]{line, HASH_LIST_FILE.getDisplayName()});
                        }
                    }
                }

                // Check if single match is desired and if a match has been made
                if (context.getProperty(MATCHING_MODE).getValue() == singleMatch.getValue() && (matched.size() > 0)) {
                    // and save time by breaking the outer loop
                    break iterateFile;
                }
            }
            // no matter if the break was called or not, Continue processing
            // First by creating a new map to hold attributes
            Map<String, String> attributes = new ConcurrentHashMap<String, String>();

            // Then by iterating over the hashmap of matches
            if (matched.size() > 0) {
                int x = 0;
                for (Map.Entry<String, Double> entry : matched.entrySet()) {
                    // defining attributes accordingly
                    attributes.put(
                            attributeName + "." + x + ".match",
                            entry.getKey());
                    attributes.put(
                            attributeName + "." + x + ".similarity",
                            String.valueOf(entry.getValue()));
                    x++;
                }
                // Finally, append the attributes to the flowfile and sent to match
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_FOUND);
                session.commit();
                return;
            } else {
                // Otherwise send it to non-match
                session.transfer(flowFile, REL_NOT_FOUND);
                session.commit();
                return;
            }
        } catch (IOException e) {
            logger.error("Error while reading the hash input source" );
            context.yield();
        }
    }



}
