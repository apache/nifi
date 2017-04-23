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

import com.idealista.tlsh.digests.Digest;
import com.idealista.tlsh.digests.DigestBuilder;
import info.debatty.java.spamsum.SpamSum;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
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
        @WritesAttribute(attribute = "XXXX.N.match", description = "The match that ressambles the attribute specified " +
                "by the <Hash Attribute Name> property. Note that: 'XXX' gets replaced with the <Hash Attribute Name>"),
        @WritesAttribute(attribute = "XXXX.N.similarity", description = "The similarity score between this flowfile" +
                "and the its match of the same number N. Note that: 'XXX' gets replaced with the <Hash Attribute Name>")})

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
            .description("The ")
            .required(true)
            .allowableValues(singleMatch,multiMatch)
            .defaultValue(singleMatch.getValue())
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("Matched")
            .description("Any FlowFile that is successfully matched to an existing hash will be sent to this Relationship.")
            .build();

    public static final Relationship REL_NON_MATCH = new Relationship.Builder()
            .name("non-match")
            .description("Any FlowFile that cannot be matched to an existing hash will be sent to this Relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
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
        relationships.add(REL_MATCH);
        relationships.add(REL_NON_MATCH);
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

        String inputHash = flowFile.getAttribute(context.getProperty(ATTRIBUTE_NAME).getValue());

        if (inputHash == null) {
            getLogger().info("FlowFile {} lacks the required '{}' attribute, routing to failure.",
                    new Object[]{flowFile, context.getProperty(ATTRIBUTE_NAME).getValue() });
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        Digest inputDigest = null;
        SpamSum spamSum = null;

        switch (algorithm) {
            case tlsh:
                // In case we are using TLSH, makes sense to create the source Digest just once
                inputDigest = compareStringToTLSHDigest(inputHash);
                // we test the validation for null (failed)
                if (inputDigest == null) {
                    // and if that is the case we log
                    logger.error("Invalid hash provided. Sending to failure");
                    //  and send to failure
                    session.transfer(flowFile, REL_FAILURE);
                    session.commit();
                    return;
                }
                break;
            case ssdeep:
                // However, in SSDEEP, the compare function uses the two desired strings.
                // So we try a poor man validation (the SpamSum comparison function seems to
                // be resilient enough but we still want to route to failure in case it
                // clearly bogus data
                if (looksLikeSpamSum(inputHash) == true) {
                    spamSum = new SpamSum();
                } else {
                    // and if that is the case we log
                    logger.error("Invalid hash provided. Sending to failure");
                    //  and send to failure
                    session.transfer(flowFile, REL_FAILURE);
                    session.commit();
                    return;
                }
        }

        File file = new File(context.getProperty(HASH_LIST_FILE).getValue());

        double similarity = 0;
        double matchThreshold = context.getProperty(MATCH_THRESHOLD).asDouble();

        try {
            Map<String, Double> matched = new ConcurrentHashMap<String, Double>();
            FileInputStream fileInputStream = new FileInputStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));

            // If SSdeep skip the first line (as the usual format used by other tools add a header line
            // to a file list
            if (algorithm == ssdeep) {
                reader.readLine();
            }

            String line = null;
            String[] hashToCompare;

            iterateFile: while ((line = reader.readLine()) != null) {
                switch (context.getProperty(HASH_ALGORITHM).getValue()) {
                    case tlsh:
                        hashToCompare = line.split("\t", 2);

                        // This will return null in case it fails validation
                        Digest digestToCompare = compareStringToTLSHDigest(hashToCompare[0]);

                        // So we test it
                        if (digestToCompare != null) {
                            similarity = inputDigest.calculateDifference(digestToCompare, true);
                            // Note how the similarity is lower of greater than
                            // This is due to TLSH 0 = perfect match, while 200 = no similarity
                            if (similarity <= matchThreshold) {
                                matched.put(hashToCompare[1], similarity);
                            }
                        }
                        break;
                    case ssdeep:
                        hashToCompare = line.split(",", 2);
                        // within the match below, one can find a quick comparison of block length, which happens to be
                        // the initial ssdeep optimisation strategy described by Brian Wallace at Virus Bulletin
                        // on 2015-11-27 https://www.virusbulletin.com/virusbulletin/2015/11/optimizing-ssdeep-use-scale
                        // The other strategy (integerDB) is better left for site specific implementations via Scripted
                        // Processors for example.
                        similarity = spamSum.match(inputHash, hashToCompare[0]);
                        if (similarity >= matchThreshold) {
                            matched.put(hashToCompare[1], similarity);
                        }
                        break;
                }
                // Check if single match is desired and if a matche has been made
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
                            context.getProperty(ATTRIBUTE_NAME).getValue() + "." + x + ".match",
                            entry.getKey());
                    attributes.put(
                            context.getProperty(ATTRIBUTE_NAME).getValue() + "." + x + ".similarity",
                            String.valueOf(entry.getValue()));
                    x++;
                }
                // Finally, append the attributes to the flowfile and sent to match
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_MATCH);
                session.commit();
                return;
            } else {
                // Otherwise send it to non-match
                session.transfer(flowFile, REL_NON_MATCH);
                session.commit();
                return;
            }


        } catch (FileNotFoundException e) {
            logger.error("Could not open the hash input file. Please check " + HASH_LIST_FILE.getDisplayName() + " setting." );
            context.yield();
        } catch (IOException e) {
            logger.error("Error while reading the hash input file" );
            context.yield();
        }
    }

    private Digest compareStringToTLSHDigest(String stringFromHashList) {
        // Because DigestBuilder raises all sort of exceptions, so in order to keep the onTrigger loop a
        // bit cleaner, we capture them here and return NaN to the loop above, otherwise simply return the
        // similarity score.
        try {
            Digest digest = new DigestBuilder().withHash(stringFromHashList).build();

            return digest;
        } catch (ArrayIndexOutOfBoundsException | StringIndexOutOfBoundsException | NumberFormatException e) {
            getLogger().error("Got {} while processing the string '{}'. This usually means the file " +
                    "defined by '{}' property contains invalid entries.",
                    new Object[]{e.getCause(), stringFromHashList, HASH_LIST_FILE.getDisplayName()});
            return null;
        }
    }

    protected boolean looksLikeSpamSum(String inputHash) {
        // format looks like
        // blocksize:hash:hash

        String [] fields = inputHash.split(":", 3);

        if (fields.length == 3) {
            Scanner sc = new Scanner(fields[0]);

            boolean isNumber = sc.hasNextInt();
            if (isNumber == false) {
                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Field should be numeric but got '{}'. Will tell processor to ignore.",
                            new Object[] {fields[0]});
                }
            }

            boolean hashOneIsNotEmpty = !fields[1].isEmpty();
            boolean hashTwoIsNotEmpty = !fields[2].isEmpty();

            if (isNumber && hashOneIsNotEmpty && hashTwoIsNotEmpty) {
                return true;
            }
        }
        return false;
    }

}
