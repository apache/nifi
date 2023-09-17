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

package org.apache.nifi.processors.enrich;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.whois.WhoisClient;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"whois", "enrich", "ip"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("A powerful whois query processor primary designed to enrich DataFlows with whois based APIs " +
        "(e.g. ShadowServer's ASN lookup) but that can be also used to perform regular whois lookups.")
@WritesAttributes({
        @WritesAttribute(attribute = "enrich.dns.record*.group*", description = "The captured fields of the Whois query response for each of the records received"),
})
public class QueryWhois extends AbstractEnrichProcessor {

    public static final AllowableValue BEGIN_END = new AllowableValue("Begin/End", "Begin/End",
            "The evaluated input of each flowfile is enclosed within begin and end tags. Each row contains a delimited set of fields");

    public static final AllowableValue BULK_NONE = new AllowableValue("None", "None",
            "Queries are made without any particular dialect");


    public static final PropertyDescriptor WHOIS_QUERY_TYPE = new PropertyDescriptor.Builder()
            .name("WHOIS_QUERY_TYPE")
            .displayName("Whois Query Type")
            .description("The Whois query type to be used by the processor (if used)")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WHOIS_SERVER = new PropertyDescriptor.Builder()
            .name("WHOIS_SERVER")
            .displayName("Whois Server")
            .description("The Whois server to be used")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WHOIS_SERVER_PORT = new PropertyDescriptor.Builder()
            .name("WHOIS_SERVER_PORT")
            .displayName("Whois Server Port")
            .description("The TCP port of the remote Whois server")
            .required(true)
            .defaultValue("43")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor WHOIS_TIMEOUT = new PropertyDescriptor.Builder()
            .name("WHOIS_TIMEOUT")
            .displayName("Whois Query Timeout")
            .description("The amount of time to wait until considering a query as failed")
            .required(true)
            .defaultValue("1500 ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("BATCH_SIZE")
            .displayName("Batch Size")
            .description("The number of incoming FlowFiles to process in a single execution of this processor. ")
            .required(true)
            .defaultValue("25")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor BULK_PROTOCOL = new PropertyDescriptor.Builder()
            .name("BULK_PROTOCOL")
            .displayName("Bulk Protocol")
            .description("The protocol used to perform the bulk query. ")
            .required(true)
            .defaultValue(BULK_NONE.getValue())
            .allowableValues(BEGIN_END, BULK_NONE)
            .build();

    @Override
    public List<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final String chosenQUERY_PARSER = validationContext.getProperty(QUERY_PARSER).getValue();

        if (!chosenQUERY_PARSER.equals(NONE.getValue())  &&  !validationContext.getProperty(QUERY_PARSER_INPUT).isSet() ) {
            results.add(new ValidationResult.Builder().input("QUERY_PARSER_INPUT")
                    .subject(QUERY_PARSER_INPUT.getDisplayName())
                    .explanation("Split and Regex parsers require a valid Regular Expression")
                    .valid(false)
                    .build());
        }


        if (validationContext.getProperty(BATCH_SIZE).asInteger() > 1 &&   !validationContext.getProperty(KEY_GROUP).isSet() )  {
            results.add(new ValidationResult.Builder().input("KEY_GROUP")
                    .subject(KEY_GROUP.getDisplayName())
                    .explanation("when operating in Batching mode, RegEx and Split parsers require a " +
                            "valid capture group/matching column. Configure the processor batch size to 1" +
                            " or enter a valid column / named capture value.")
                    .valid(false)
                    .build());
        }

        if ( validationContext.getProperty(BATCH_SIZE).asInteger() > 1  && chosenQUERY_PARSER.equals(NONE.getValue())  ) {
            results.add(new ValidationResult.Builder().input(validationContext.getProperty(BATCH_SIZE).getValue())
                    .subject(QUERY_PARSER.getDisplayName())
                    .explanation("NONE parser does not support batching. Configure Batch Size to 1 or use another parser.")
                    .valid(false)
                    .build());
        }

        if ( validationContext.getProperty(BATCH_SIZE).asInteger() == 1  && !validationContext.getProperty(BULK_PROTOCOL).getValue().equals(BULK_NONE.getValue()) ) {
            results.add(new ValidationResult.Builder().input("BULK_PROTOCOL")
                    .subject(BATCH_SIZE.getDisplayName())
                    .explanation("Bulk protocol requirement requires batching. Configure Batch Size to more than 1 or " +
                            "use another protocol.")
                    .valid(false)
                    .build());
        }



        return results;
    }

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    private WhoisClient whoisClient;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(QUERY_INPUT);
        props.add(WHOIS_QUERY_TYPE);
        props.add(WHOIS_SERVER);
        props.add(WHOIS_SERVER_PORT);
        props.add(WHOIS_TIMEOUT);
        props.add(BATCH_SIZE);
        props.add(BULK_PROTOCOL);
        props.add(QUERY_PARSER);
        props.add(QUERY_PARSER_INPUT);
        props.add(KEY_GROUP);
        propertyDescriptors = Collections.unmodifiableList(props);

        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_FOUND);
        rels.add(REL_NOT_FOUND);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();

        List<FlowFile> flowFiles = session.get(batchSize);

        if (flowFiles == null || flowFiles.isEmpty()) {
            context.yield();
            return;
        }

        // Build query
        String buildString = "";
        final String queryType = context.getProperty(WHOIS_QUERY_TYPE).getValue();

        // Verify the the protocol mode and craft the "begin" pseudo-command, otherwise just the query type
        buildString = context.getProperty(BULK_PROTOCOL).getValue().equals(BEGIN_END.getValue())  ? buildString.concat("begin ") : buildString.concat("");

        // Append the query type
        buildString = context.getProperty(WHOIS_QUERY_TYPE).isSet()  ? buildString.concat(queryType + " " ) : buildString.concat("");

        // A new line is required when working on Begin/End
        buildString = context.getProperty(BULK_PROTOCOL).getValue().equals(BEGIN_END.getValue()) ? buildString.concat("\n") : buildString.concat("");

        // append the values
        for (FlowFile flowFile : flowFiles) {
            final String evaluatedInput = context.getProperty(QUERY_INPUT).evaluateAttributeExpressions(flowFile).getValue();
            buildString = buildString + evaluatedInput + "\n";
        }

        // Verify the the protocol mode and craft the "end" pseudo-command, otherwise just the query type
        buildString = context.getProperty(BULK_PROTOCOL).getValue().equals(BEGIN_END.getValue())  ? buildString.concat("end") : buildString.concat("");


        final String queryParser = context.getProperty(QUERY_PARSER).getValue();
        final String queryRegex = context.getProperty(QUERY_PARSER_INPUT).getValue();
        final int keyLookup = context.getProperty(KEY_GROUP).asInteger();
        final int whoisTimeout = context.getProperty(WHOIS_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final String whoisServer = context.getProperty(WHOIS_SERVER).getValue();
        final int whoisPort = context.getProperty(WHOIS_SERVER_PORT).asInteger();

        final List<FlowFile> flowFilesMatched = new ArrayList<FlowFile>();
        final List<FlowFile> flowFilesNotMatched = new ArrayList<FlowFile>();


        String result = doLookup(whoisServer, whoisPort, whoisTimeout, buildString);
        if (StringUtils.isEmpty(result)) {
            // If nothing was returned, let the processor continue its life and transfer the batch to REL_NOT_FOUND
            session.transfer(flowFiles, REL_NOT_FOUND);
            return;
        } else {
            // Run as normal
            for (FlowFile flowFile : flowFiles) {
                // Check the batchSize. If 1, run normal parser
                if (batchSize == 1) {

                    Map<String, String> parsedResults = parseResponse(null, result, queryParser, queryRegex, "whois");

                    if (parsedResults.isEmpty()) {
                        // parsedResults didn't return anything valid, sending to not found.
                        flowFilesNotMatched.add(flowFile);
                    } else {
                        // Still, extraction is needed
                        flowFile = session.putAllAttributes(flowFile, parsedResults);
                        flowFilesMatched.add(flowFile);

                        // Finished processing single result
                    }
                } else {
                    // Otherwise call the multiline parser and get the row map;
                    final Map<String, Map<String, String>> rowMap = parseBatchResponse(result, queryParser, queryRegex, keyLookup, "whois");

                    // Identify the flowfile Lookupvalue and search against the rowMap
                    String ffLookupValue = context.getProperty(QUERY_INPUT).evaluateAttributeExpressions(flowFile).getValue();

                    if (rowMap.containsKey(ffLookupValue)) {
                        // flowfile Lookup Value is contained within the results, get the properties and add to matched list
                        flowFile = session.putAllAttributes(flowFile, rowMap.get(ffLookupValue));
                        flowFilesMatched.add(flowFile);
                    } else {
                        // otherwise add to Not Matched
                        flowFilesNotMatched.add(flowFile);
                    }
                }
            }
        }

        // Finally prepare to send the data down the pipeline
        // Because batches may include matches and non-matches, test both and send
        // each to its relationship
        if (flowFilesMatched.size() > 0) {
            // Sending the resulting flowfile (with attributes) to REL_FOUND
            session.transfer(flowFilesMatched, REL_FOUND);
        }
        if (flowFilesNotMatched.size() > 0) {
            // Sending whatetver didn't match to REL_NOT_FOUND
            session.transfer(flowFilesNotMatched, REL_NOT_FOUND);
        }
    }

    /**
     * This method performs a simple Whois lookup
     * @param whoisServer Server to be queried;
    *  @param whoisPort TCP port to be useed to connect to server
     * @param whoisTimeout How long to wait for a response (in ms);
     * @param query The query to be made;
     */
    protected String doLookup(String whoisServer, int whoisPort, int whoisTimeout, String query) {
        // This is a simple WHOIS lookup attempt

        String result = null;

        whoisClient = createClient();

        try {
            // Uses pre-existing context to resolve
            if (!whoisClient.isConnected()) {
                whoisClient.connect(whoisServer, whoisPort);
                whoisClient.setSoTimeout(whoisTimeout);
                result = whoisClient.query(query);
                // clean up...
                if (whoisClient.isConnected()) whoisClient.disconnect();
            }
        } catch ( IOException e) {
            getLogger().error("Query failed due to {}", e.getMessage(), e);
            throw new ProcessException("Error performing Whois Lookup", e);
        }
        return result;
    }


    /*
    Note createClient() was separated from the rest of code
    in order to allow powermock to inject a fake return
    during testing
     */
    protected WhoisClient createClient() {
        return new WhoisClient();
    }
}
