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


import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractEnrichProcessor extends AbstractProcessor {
    public static final PropertyDescriptor QUERY_INPUT = new PropertyDescriptor.Builder()
            .name("QUERY_INPUT")
            .displayName("Lookup value")
            .required(true)
            .description("The value that should be used to populate the query")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final AllowableValue SPLIT= new AllowableValue("Split", "Split",
            "Use a delimiter character or RegEx  to split the results into attributes");
    public static final AllowableValue REGEX = new AllowableValue("RegEx", "RegEx",
            "Use a regular expression to split the results into attributes ");
    public static final AllowableValue NONE = new AllowableValue("None", "None",
            "Do not split results");

    public static final PropertyDescriptor QUERY_PARSER = new PropertyDescriptor.Builder()
            .name("QUERY_PARSER")
            .displayName("Results Parser")
            .description("The method used to slice the results into attribute groups")
            .allowableValues(SPLIT, REGEX, NONE)
            .required(true)
            .defaultValue(NONE.getValue())
            .build();

    public static final PropertyDescriptor QUERY_PARSER_INPUT = new PropertyDescriptor.Builder()
            .name("QUERY_PARSER_INPUT")
            .displayName("Parser RegEx")
            .description("Choice between a splitter and regex matcher used to parse the results of the query into attribute groups")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final Relationship REL_FOUND = new Relationship.Builder()
            .name("found")
            .description("Where to route flow files after successfully enriching attributes with data")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("Where to route flow files if data enrichment query rendered no results")
            .build();


    @Override
    public List<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final String chosenQUERY_PARSER = validationContext.getProperty(QUERY_PARSER).getValue();
        final boolean QUERY_PARSER_INPUT_isSet = validationContext.getProperty(QUERY_PARSER_INPUT).isSet();

        if ((!chosenQUERY_PARSER.equals(NONE.getValue()) ) && ( !QUERY_PARSER_INPUT_isSet )) {
            results.add(new ValidationResult.Builder().input("QUERY_PARSER_INPUT")
                    .explanation("Split and Regex parsers require a valid Regular Expression")
                    .valid(false)
                    .build());
        }

        if ((chosenQUERY_PARSER.equals(NONE.getValue()) ) && ( QUERY_PARSER_INPUT_isSet )) {
            results.add(new ValidationResult.Builder().input("QUERY_PARSER_INPUT")
                    .explanation("NONE parser does not support the use of Regular Expressions")
                    .valid(false)
                    .build());
        }

        return results;
    }



    /**
     * This method returns the parsed record string in the form of
     * a map of two strings, consisting of a iteration aware attribute
     * names and its values
     *
     * @param  recordPosition  the iteration counter for the record
     * @param  rawResult the raw query results to be parsed
     * @param queryParser The parsing mechanism being used to parse the data into groups
     * @param queryRegex The regex to be used to split the query results into groups
     * @return  Map with attribute names and values
     */
    protected Map<String, String> parseResponse(int recordPosition, String rawResult, String queryParser, String queryRegex, String schema) {

        Map<String, String> results = new HashMap<>();
        Pattern p;


        // Iterates  over the results using the QUERY_REGEX adding the captured groups
        // as it progresses

        switch (queryParser) {
            case "Split":
                // Time to Split the results...
                String[] splitResult = rawResult.split(queryRegex);
                for (int r = 0; r < splitResult.length; r++) {
                    results.put("enrich." + schema + ".record" + String.valueOf(recordPosition) + ".group" + String.valueOf(r), splitResult[r]);
                }
                break;

            case "RegEx":
                // RegEx was chosen, iterating...
                p = Pattern.compile(queryRegex);
                Matcher finalResult = p.matcher(rawResult);
                if (finalResult.find()) {
                    // Note that RegEx matches capture group 0 is usually broad but starting with it anyway
                    // for the sake of purity
                    for (int r = 0; r < finalResult.groupCount(); r++) {
                        results.put("enrich." + schema + ".record" + String.valueOf(recordPosition) + ".group" + String.valueOf(r), finalResult.group(r));
                    }
                }
                break;

            case "None":
                // Fails to NONE
            default:
                // NONE was chosen, just appending the record result as group0 without further splitting
                results.put("enrich." + schema + ".record" + String.valueOf(recordPosition) + ".group0", rawResult);
                break;
        }
        return results;
    }

}
