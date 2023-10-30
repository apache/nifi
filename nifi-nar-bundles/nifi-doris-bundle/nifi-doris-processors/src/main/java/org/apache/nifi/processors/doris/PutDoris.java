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
package org.apache.nifi.processors.doris;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.JSONPObject;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpPut;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.doris.DorisClientService;
import org.apache.nifi.doris.util.Result;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

@Tags({"doris"})
@CapabilityDescription("Provide a description test test")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class PutDoris extends AbstractProcessor {

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    protected DorisClientService dorisClientService;
    protected HashMap<String, String> oneToOneMap = new HashMap<>();
    private volatile int batchSize;
    private String srcDatabaseName;
    private String srcTableName;
    private String deleteSign;

    private String op;

    public static final PropertyDescriptor DORIS_CLIENT_SERVICE = new PropertyDescriptor
            .Builder().name("DORIS_CLIENT_SERVICE")
            .displayName("doris_client_service")
            .description("DORIS_CLIENT_SERVICE")
            .required(true)
            .identifiesControllerService(DorisClientService.class)
            .build();

    public static final PropertyDescriptor SRC_DATABASE_NAME_KEY = new PropertyDescriptor
            .Builder().name("DEST_DATABASE")
            .displayName("dest_database")
            .description("DEST_DATABASE")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("database")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SRC_TABLE_NAME_KEY = new PropertyDescriptor
            .Builder().name("SRC_TABLE_NAME_KEY")
            .displayName("src_table_name_key")
            .description("SRC_TABLE_NAME_KEY")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("table_name")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DELETE_SIGN = new PropertyDescriptor
            .Builder().name("DELETE_SIGN")
            .displayName("delete_sign")
            .description("DELETE_SIGN")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("__DORIS_DELETE_SIGN__")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor OP = new PropertyDescriptor
            .Builder().name("OP")
            .displayName("op")
            .description("OP")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("op")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are sent to the AMQP destination are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be routed to the AMQP destination are routed to this relationship")
            .build();

    public static final PropertyDescriptor ONETOONE = new PropertyDescriptor
            .Builder().name("one-to-one")
            .displayName("one-to-one")
            .description("one-to-one")
            .required(false)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("(\\w+\\.\\w+:\\w+\\.\\w+)(,\\w+\\.\\w+:\\w+\\.\\w+)*")))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("BATCH_SIZE")
            .description("The maximum number of FlowFiles to process in a single execution, between 1 - 100000. " +
                    "Depending on your memory size, and data size per row set an appropriate batch size " +
                    "for the number of FlowFiles to process per client connection setup." +
                    "Gradually increase this number, only if your FlowFiles typically contain a few records.")
            .defaultValue("1")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 100000, true))
            .expressionLanguageSupported(true)
            .build();


    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(DORIS_CLIENT_SERVICE);
        descriptors.add(ONETOONE);
        descriptors.add(BATCH_SIZE);
        descriptors.add(SRC_DATABASE_NAME_KEY);
        descriptors.add(SRC_TABLE_NAME_KEY);
        descriptors.add(DELETE_SIGN);
        descriptors.add(OP);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
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
        dorisClientService = context.getProperty(DORIS_CLIENT_SERVICE).asControllerService(DorisClientService.class);
        batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        srcDatabaseName = context.getProperty(SRC_DATABASE_NAME_KEY).evaluateAttributeExpressions().getValue();
        srcTableName = context.getProperty(SRC_TABLE_NAME_KEY).evaluateAttributeExpressions().getValue();
        deleteSign = context.getProperty(DELETE_SIGN).evaluateAttributeExpressions().getValue();
        op = context.getProperty(OP).evaluateAttributeExpressions().getValue();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles.isEmpty()) {
            return;
        }

        final Map<String, List<DorisFlowFile>> dorisFlowFiles = new HashMap<>();
        for (final FlowFile flowFile : flowFiles) {

            final ObjectMapper mapper = new ObjectMapper();
            final AtomicReference<List<JsonNode>> rootNodeRef = new AtomicReference<>(null);

            session.read(flowFile, in -> {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
                String line;
                ArrayList<JsonNode> jnList = new ArrayList<>();
                try {
                    while ((line = bufferedReader.readLine()) != null) {
                        JsonNode newNode = mapper.readTree(line);
                        ObjectNode objectNode = (ObjectNode) newNode;

                        if (null != objectNode.get(op) && "delete".equalsIgnoreCase(objectNode.get(op).asText())) {
                            objectNode.put(deleteSign, true);
                            jnList.add(objectNode);
                        } else {
                            objectNode.put(deleteSign, false);
                            jnList.add(objectNode);
                        }
                    }


                } catch (ProcessException pe) {
                    //TODO:
                } finally {
                    bufferedReader.close();
                }
            });

            for (JsonNode jsonNode : rootNodeRef.get()) {
                Iterator<String> fieldNames = jsonNode.fieldNames();
                StringBuffer fieldNamesList = new StringBuffer();

                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    if (srcDatabaseName.equalsIgnoreCase(fieldName) || srcTableName.equalsIgnoreCase(fieldName) || deleteSign.equalsIgnoreCase(fieldName) || op.equalsIgnoreCase(fieldName)) {
                        continue;
                    }
                    fieldNamesList.append("`" + fieldName + "`" + ",");
                }
                DorisFlowFile dorisFlowFile = new DorisFlowFile();
                //del database and table_name
                if (jsonNode.isObject()) {
                    ObjectNode objectNode = (ObjectNode) jsonNode;

                    dorisFlowFile.setSrcDatabaseName(jsonNode.get(srcDatabaseName).asText());
                    dorisFlowFile.setSrcTableName(jsonNode.get(srcTableName).asText());
                    dorisFlowFile.setColumns(fieldNamesList.toString().concat(deleteSign));
                    dorisFlowFile.setFlowFile(flowFile);

                    objectNode.remove(srcDatabaseName);
                    objectNode.remove(srcTableName);
                    objectNode.remove(op);
                    dorisFlowFile.setJsonNode(jsonNode);
                } else {
                    dorisFlowFile = null;
                }

                if (dorisFlowFile == null) {
                    session.transfer(flowFile, REL_FAILURE);
                }

                List<DorisFlowFile> dff = dorisFlowFiles.get(dorisFlowFile.getSrcDatabaseName().concat(".").concat(dorisFlowFile.getSrcTableName()));

                if (dff == null) {
                    dff = new ArrayList<>();
                    dorisFlowFiles.put(dorisFlowFile.getSrcDatabaseName().concat(".").concat(dorisFlowFile.getSrcTableName()), dff);
                }
                dff.add(dorisFlowFile);

            }

        }
        final List<FlowFile> successes = new ArrayList<>();


        if (context.getProperty(ONETOONE).isSet()) {
            //srcDatabase.srcTable:destDatabase.destTable,srcDatabase.srcTable1:destDatabase.destTable1,foo...
            String oneToOneValue = context.getProperty(ONETOONE).evaluateAttributeExpressions().getValue();
            String[] split = oneToOneValue.split(",", -1);
            for (String s : split) {
                String[] databaseAndTable = s.split(":", -1);
                oneToOneMap.put(databaseAndTable[0], databaseAndTable[1]);
            }
        }

        //key -> destDbNameAndDestTabName , value -> List<>(jsonData)
        HashMap<String, List<String>> putJsonMap = new HashMap<>();

        /**
         *It is possible that a flowfile contains data for multiple tables, so if a table fails, you need to capture the flowfile for failure handling;
         *If there are multiple tables in multiple flowfiles and one of the tables fails, all flowfiles corresponding to this table need to be obtained to handle the failure.
         * The id of each flowfile determines whether the table information is from a flowfile
         */
        //key -> destDbNameAndDestTabName , value -> List<>(FlowFile)
        HashMap<String, List<FlowFile>> flowFileMap = new HashMap<>();

        for (Map.Entry<String, List<DorisFlowFile>> stringListEntry : dorisFlowFiles.entrySet()) {

            String srcDbAndTab = stringListEntry.getKey();
            String destDbAndTab = oneToOneMap.getOrDefault(srcDbAndTab, srcDbAndTab);

            List<DorisFlowFile> value = stringListEntry.getValue();

            for (DorisFlowFile dorisFlowFile : value) {
                //init client
                String destDatabase = destDbAndTab.split("\\.", -1)[0];
                String destTableName = destDbAndTab.split("\\.", -1)[1];
                dorisClientService.setClient(destDatabase, destTableName, dorisFlowFile.getColumns());

                //Build the putJsonMap
                List<String> jsonList = putJsonMap.get(destDbAndTab);
                if (null == jsonList) {
                    jsonList = new ArrayList<>();
                    putJsonMap.put(destDbAndTab, jsonList);
                }
                jsonList.add(dorisFlowFile.getJsonNode().toString());

                //Build the flowFileMap
                List<FlowFile> flowFilesCurrent = flowFileMap.get(destDbAndTab);
                if (null == flowFilesCurrent) {
                    flowFilesCurrent = new ArrayList<>();
                    flowFileMap.put(destDbAndTab, flowFilesCurrent);
                }
                flowFilesCurrent.add(dorisFlowFile.getFlowFile());
            }

        }


        for (Map.Entry<String, List<FlowFile>> stringListEntry : flowFileMap.entrySet()) {
            String key = stringListEntry.getKey();
            List<FlowFile> flowFiles1 = flowFileMap.get(key);
            List<FlowFile> flowFiles2 = removeDuplicateWithOrder(flowFiles1);

            for (FlowFile flowFile : flowFiles2) {
                System.out.println("key = " + key + "-> value = " + flowFile);
            }


        }

        //Write the data to doris
        for (Map.Entry<String, List<String>> stringListEntry : putJsonMap.entrySet()) {

            String key = stringListEntry.getKey();

            try {
                String destDatabase = key.split("\\.", -1)[0];
                String destTableName = key.split("\\.", -1)[1];
                Result insert = dorisClientService.insert(stringListEntry.getValue().toString(), destDatabase, destTableName);
                successes.addAll(flowFileMap.get(key));
            } catch (Exception e) {
                session.rollback(true);
                for (FlowFile flowFile : flowFileMap.get(key)) {
                    session.transfer(session.penalize(flowFile), REL_FAILURE);
                }
            }
        }

        for (Map.Entry<String, List<DorisFlowFile>> stringListEntry : dorisFlowFiles.entrySet()) {
            for (FlowFile flowFile : successes) {
                session.transfer(flowFile, REL_SUCCESS);
                //final String details = "Put " + dorisFlowFile.getColumns().size() + " cells to HBase";
                //session.getProvenanceReporter().send(dorisFlowFile.getFlowFile(), getTransitUri(putFlowFile), details, sendMillis);
            }
            //session.transfer(flowFile, REL_SUCCESS);


        }
    }

    public static List<FlowFile> removeDuplicateWithOrder(List<FlowFile> list) {
        Set set = new HashSet();
        List newList = new ArrayList();
        for (Iterator iter = list.iterator(); iter.hasNext(); ) {
            Object element = iter.next();
            if (set.add(element))
                newList.add(element);
        }
        list.clear();
        list.addAll(newList);
        return list;
    }
}
