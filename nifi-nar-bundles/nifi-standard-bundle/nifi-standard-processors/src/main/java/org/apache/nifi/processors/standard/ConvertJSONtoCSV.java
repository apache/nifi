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
package org.apache.nifi.processors.standard;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"JSON, CSV, convert"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttribute(attribute="mime.type", description="Sets the mime type to application/csv")
@CapabilityDescription("Converts a JSON document to CSV. This processor reads the entire content " +
        "of incoming FlowFiles into memory in order to perform the conversion. The processor will parse JSON Arrays, JSON Objects " +
        "and the combination of the two regardless of the level of nesting in the JSON document.")
public class ConvertJSONtoCSV extends AbstractProcessor {
    volatile String delimiter;
    volatile String removeFields;
    volatile String emptyFields = "";

    public static final AllowableValue INCLUDE_HEADER_TRUE = new AllowableValue(
            "True", "True", "Creates headers for each JSON file.");
    public static final AllowableValue INCLUDE_HEADER_FALSE = new AllowableValue(
            "False", "False", "Only parses the JSON fields and does not include headers");


    public static final PropertyDescriptor DELIMITER = new PropertyDescriptor
            .Builder().name("CSV Delimiter")
            .description("Delimiter used for the generated CSV output (Example: , | -)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REMOVE_FIELDS = new PropertyDescriptor
            .Builder().name("Remove JSON Fields/Columns")
            .description("Comma delimited list of columns that should be removed when parsing JSON and building the CSV. " +
                    "This includes all top level and most granular nested fields/columns. By default with nothing specified every field " +
                    "will be parsed.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor EMPTY_FIELDS = new PropertyDescriptor
            .Builder().name("Empty field value")
            .description("During denormalization/flattening of the JSON the value that will be substituted for empty fields values " +
                    "(Example: NULL). Defaults to empty string if not specified.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_HEADERS = new PropertyDescriptor
            .Builder().name("Include Headers")
            .description("Whether or not to include headers in the CSV output")
            .required(true)
            .allowableValues(INCLUDE_HEADER_TRUE, INCLUDE_HEADER_FALSE)
            .defaultValue(INCLUDE_HEADER_TRUE.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully parsing the JSON file to CSV ")
            .build();

    public static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed parsing the JSON file to CSV ")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(DELIMITER);
        descriptors.add(REMOVE_FIELDS);
        descriptors.add(EMPTY_FIELDS);
        descriptors.add(INCLUDE_HEADERS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(RELATIONSHIP_SUCCESS);
        relationships.add(RELATIONSHIP_FAILURE);
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

        delimiter = context.getProperty(DELIMITER).getValue();
        removeFields = context.getProperty(REMOVE_FIELDS).getValue() == null ? "" : context.getProperty(REMOVE_FIELDS).getValue();
        emptyFields = context.getProperty(EMPTY_FIELDS).getValue();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AtomicReference<String> jsonHolder = new AtomicReference<>();
        final String includeHeaders = context.getProperty(INCLUDE_HEADERS).getValue();

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        try {
            session.read(flowFile, (new InputStreamCallback() {
                @Override
                public void process(InputStream inputStream) throws IOException {
                    List<Map<String, String>> flatJson =
                            JSONParser.parseJSON(IOUtils.toString(inputStream, "UTF-8"), getRemoveKeySet());
                    try {
                        if (flatJson == null) {
                            throw new IOException("Unable to parse JSON file. Please check the file contains valid JSON structure");
                        }
                        jsonHolder.set(CSVGenerator.generateCSV(flatJson, delimiter, emptyFields, includeHeaders).toString());
                    } catch (JSONException ex) {
                        throw new JSONException("Unable to parse as JSON appears to be malformed: " + ex);
                    }
                }
            }));

            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream outputStream) throws IOException {
                    outputStream.write(jsonHolder.get().getBytes());
                }
            });

            session.transfer(flowFile, RELATIONSHIP_SUCCESS);
        } catch (ProcessException | JSONException ex) {
            getLogger().error("Error converting FlowFile to CSV due to {}", new Object[] {ex.getMessage()}, ex);
            session.transfer(flowFile, RELATIONSHIP_FAILURE);
        }

    }

    private Set<String> getRemoveKeySet() {
        Set<String> setRemove = new HashSet<>();

        final String[] fields = removeFields.split(",");
        for (String field : fields) {
            setRemove.add(field);
        }
        return setRemove;
    }

    private static class JSONParser {

        private static final Class<?> JSON_OBJECT = JSONObject.class;

        private static final Class<?> JSON_ARRAY = JSONArray.class;

        private static Set<String> removeKeySet = new HashSet<>();

        public static List<Map<String, String>> parseJSON(String json, Set<String> removeKeys) throws JSONException {
            List<Map<String, String>> flatJson = null;
            removeKeySet = removeKeys;

            // we don't know if we are dealing with a JSON object or an array
            // not handling this properly causes un-necessary exception to be thrown
            // instantiate to a generic object and check what we are dealing with
            Object objJSON = new JSONTokener(json).nextValue();

            try {
                if (objJSON instanceof JSONObject) {
                    JSONObject jsonObject = new JSONObject(json);
                    flatJson = new ArrayList<Map<String, String>>();
                    flatJson.add(parse(jsonObject));
                } else if (objJSON instanceof JSONArray) {
                    flatJson = handleAsArray(json);
                }
            } catch (JSONException ex) {
                throw new JSONException("JSON might be malformed, please check and make sure it is valid JSON format: " + ex);
            }

            return flatJson;
        }

        public static Map<String, String> parse(JSONObject jsonObject) {
            Map<String, String> flatJson = new LinkedHashMap<String, String>();
            try {
                denormalize(jsonObject, flatJson, "");
            } catch (Exception ex) {
                throw ex;
            }
            return flatJson;
        }

        public static List<Map<String, String>> parse(JSONArray jsonArray) {
            JSONObject jsonObject = null;
            List<Map<String, String>> flatJson = new ArrayList<Map<String, String>>();
            int length = jsonArray.length();

            for (int i = 0; i < length; i++) {
                jsonObject = jsonArray.getJSONObject(i);
                Map<String, String> stringMap = parse(jsonObject);
                flatJson.add(stringMap);
            }

            return flatJson;
        }

        private static List<Map<String, String>> handleAsArray(String json) throws JSONException {
            List<Map<String, String>> flatJson = null;

            try {
                JSONArray jsonArray = new JSONArray(json);
                flatJson = parse(jsonArray);
            } catch (JSONException e) {
                throw e;
            }

            return flatJson;
        }

        private static void denormalize(JSONObject obj, Map<String, String> flatJson, String denormalizedColumn) {
            obj.keySet().removeAll(removeKeySet);
            Iterator<?> iterator = obj.keys();
            String _denormalizedColumn = denormalizedColumn != "" ? denormalizedColumn + "." : "";

            while (iterator.hasNext()) {
                String key = iterator.next().toString();

                if (obj.get(key).getClass() == JSON_OBJECT) {
                    JSONObject jsonObject = (JSONObject) obj.get(key);
                    denormalize(jsonObject, flatJson, _denormalizedColumn + key);
                } else if (obj.get(key).getClass() == JSON_ARRAY) {
                    JSONArray jsonArray = (JSONArray) obj.get(key);

                    if (jsonArray.length() < 1) {
                        continue;
                    }

                    denormalize(jsonArray, flatJson, _denormalizedColumn + key);
                } else {
                    String value = obj.get(key).toString();

                    if (value != null && !value.equals("null")) {
                        flatJson.put(_denormalizedColumn + key, value);
                    }
                }
            }

        }

        private static void denormalize(JSONArray obj, Map<String, String> flatJson, String denormalizedColumn) {
            int length = obj.length();

            for (int i = 0; i < length; i++) {
                if (obj.get(i).getClass() == JSON_ARRAY) {
                    JSONArray jsonArray = (JSONArray) obj.get(i);

                    if (jsonArray.length() < 1) {
                        continue;
                    }

                    denormalize(jsonArray, flatJson, denormalizedColumn + "[" + i + "]");
                } else if (obj.get(i).getClass() == JSON_OBJECT) {
                    JSONObject jsonObject = (JSONObject) obj.get(i);
                    denormalize(jsonObject, flatJson, denormalizedColumn + "[" + (i + 1) + "]");
                } else {
                    String value = obj.get(i).toString();

                    if (value != null) {
                        flatJson.put(denormalizedColumn + "[" + (i + 1) + "]", value);
                    }
                }
            }
        }
    }

    private static class CSVGenerator {

        public static StringBuilder generateCSV(List<Map<String, String>> flatJson, String separator, String emptyFields, String includeHeaders) {
            Set<String> headers = createHeaders(flatJson);
            StringBuilder csvData = new StringBuilder();

            if (INCLUDE_HEADER_TRUE.equals(includeHeaders)) {
                csvData.append(StringUtils.join(headers.toArray(), separator) + "\n");
            }

            for (Map<String, String> map : flatJson) {
                csvData.append(getSeperatedColumns(headers, map, separator, emptyFields) + "\n");
            }

            return csvData;
        }

        private static String getSeperatedColumns(Set<String> headers, Map<String, String> map, String separator, String emptyFields) {
            List<String> items = new ArrayList<String>();
            for (String header : headers) {
                String value = map.get(header) == null ? "" : map.get(header).replace(",", "");
                items.add(value.isEmpty() ? emptyFields : value);
            }

            return StringUtils.join(items.toArray(), separator);
        }

        private static SortedSet<String> createHeaders(List<Map<String, String>> flatJson) {
            SortedSet<String> headers = new TreeSet();
            for (Map<String, String> map : flatJson) {
                headers.addAll(map.keySet());
            }
            return headers;
        }
    }
}

