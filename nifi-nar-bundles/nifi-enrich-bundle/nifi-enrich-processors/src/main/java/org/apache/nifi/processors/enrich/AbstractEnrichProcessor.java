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



import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractEnrichProcessor extends AbstractProcessor {
    public static final PropertyDescriptor QUERY_INPUT = new PropertyDescriptor.Builder()
            .name("QUERY_INPUT")
            .displayName("Lookup value")
            .required(true)
            .description("The value that should be used to populate the query")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
            .description("Choice between a splitter and regex matcher used to parse the results of the query into attribute groups.\n" +
            "NOTE: This is a multiline regular expression, therefore, the DFM should decide how to handle trailing new line " +
            "characters.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_GROUP = new PropertyDescriptor.Builder()
            .name("KEY_GROUP")
            .displayName("Key lookup group (multiline / batch)")
            .description("When performing a batched lookup, the following RegEx numbered capture group or Column number will be used to match " +
                    "the whois server response with the lookup field")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
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

        if (!chosenQUERY_PARSER.equals(NONE.getValue())  &&  !validationContext.getProperty(QUERY_PARSER_INPUT).isSet() ) {
            results.add(new ValidationResult.Builder().input("QUERY_PARSER_INPUT")
                    .subject(QUERY_PARSER.getDisplayName())
                    .explanation("Split and Regex parsers require a valid Regular Expression")
                    .valid(false)
                    .build());
        }

        if (chosenQUERY_PARSER.equals(NONE.getValue()) && validationContext.getProperty(QUERY_PARSER_INPUT).isSet()) {
            results.add(new ValidationResult.Builder().input("QUERY_PARSER")
                    .subject(QUERY_PARSER_INPUT.getDisplayName())
                    .explanation("NONE parser does not support the use of Regular Expressions. " +
                            "Please select another parser or delete the regular expression entered in this field.")
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
    protected Map<String, String> parseResponse(String recordPosition, String rawResult, String queryParser, String queryRegex, String schema) {

        Map<String, String> results = new HashMap<>();
        Pattern p;
        recordPosition = StringUtils.isEmpty(recordPosition) ? "0" : recordPosition;

        // Iterates  over the results using the QUERY_REGEX adding the captured groups
        // as it progresses

        switch (queryParser) {
            case "Split":
                // Time to Split the results...
                String[] splitResult = rawResult.split(queryRegex);
                for (int r = 0; r < splitResult.length; r++) {
                    results.put("enrich." + schema + ".record" + recordPosition + ".group" + String.valueOf(r), splitResult[r]);
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
                        results.put("enrich." + schema + ".record" + recordPosition + ".group" + String.valueOf(r), finalResult.group(r));
                    }
                }
                break;

            case "None":
                // Fails to NONE
            default:
                // NONE was chosen, just appending the record result as group0 without further splitting
                results.put("enrich." + schema + ".record" + recordPosition + ".group0", rawResult);
                break;
        }
        return results;
    }

    /**
     * This method returns the parsed record string in the form of
     * a map of two strings, consisting of a iteration aware attribute
     * names and its values
     *

     * @param  rawResult the raw query results to be parsed
     * @param queryParser The parsing mechanism being used to parse the data into groups
     * @param queryRegex The regex to be used to split the query results into groups. The regex MUST implement at least on named capture group "KEY" to be used to populate the table rows
     * @param lookupKey The regular expression number or the column of a split to be used for matching
     * @return  Table with attribute names and values where each Table row uses the value of the KEY named capture group specified in @param queryRegex
     */
    protected Table<String, String, String> parseBatchResponse(String rawResult, String queryParser, String queryRegex, int lookupKey, String schema) {
        // Note the hardcoded record0.
        //  Since iteration is done within the parser and Multimap is used, the record number here will always be 0.
        // Consequentially, 0 is hardcoded so that batched and non batched attributes follow the same naming
        // conventions
        final String recordPosition = ".record0";

        final Table<String, String, String> results = HashBasedTable.create();

        switch (queryParser) {
            case "Split":
                Scanner scanner = new Scanner(rawResult);
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    // Time to Split the results...
                    String[] splitResult = line.split(queryRegex);

                    for (int r = 0; r < splitResult.length; r++) {
                        results.put(splitResult[ lookupKey - 1 ], "enrich." + schema + recordPosition + ".group" + String.valueOf(r), splitResult[r]);
                    }
                }
                break;
            case "RegEx":
            // prepare the regex
            Pattern p;
            // Regex is multiline. Each line should include a KEY for lookup
            p = Pattern.compile(queryRegex, Pattern.MULTILINE);

            Matcher matcher = p.matcher(rawResult);
            while (matcher.find()) {
                try {
                    // Note that RegEx matches capture group 0 is usually broad but starting with it anyway
                    // for the sake of purity
                    for (int r = 0; r <= matcher.groupCount(); r++) {
                        results.put(matcher.group(lookupKey), "enrich." + schema + recordPosition + ".group" + String.valueOf(r), matcher.group(r));
                    }
                } catch (IndexOutOfBoundsException e) {
                    getLogger().warn("Could not find capture group {} while processing result. You may want to review your " +
                            "Regular Expression to match against the content \"{}\"", new Object[]{lookupKey, rawResult});
                }
            }
            break;
        }

        return results;
    }
}
