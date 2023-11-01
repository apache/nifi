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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.doris.DorisClientService;
import org.apache.nifi.doris.util.Result;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.ProcessSession;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

@Tags({"Doris","CDC","StreamLoad","Apache Doris"})
@CapabilityDescription("StreamLoad the json data to Apache Doris")
@SupportsBatching
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

    final ObjectMapper mapper = new ObjectMapper();

    public static final PropertyDescriptor DORIS_CLIENT_SERVICE = new PropertyDescriptor
            .Builder().name("Doris Client Service")
            .description("Used to create a Doris client, where the Doris client configuration needs to be configured")
            .required(true)
            .identifiesControllerService(DorisClientService.class)
            .build();

    public static final PropertyDescriptor SRC_DATABASE_NAME_KEY = new PropertyDescriptor
            .Builder().name("Src Database Name Key")
            .description("The key in json data, the default value is database, " +
                    "if your data does not come from a relational database (perhaps a kafka stream or other)," +
                    " you can customize this field. See the ONETOONE configuration explanation for details")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("database")
            .build();

    public static final PropertyDescriptor SRC_TABLE_NAME_KEY = new PropertyDescriptor
            .Builder().name("Src Table Name Key")
            .description("The key in json data, the default value is table_name, " +
                    "if your data does not come from a relational database (perhaps a kafka stream or other)," +
                    " you can customize this field. See the ONETOONE configuration explanation for details")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("table_name")
            .build();

    public static final PropertyDescriptor DELETE_SIGN = new PropertyDescriptor
            .Builder().name("Delete Sign")
            .description("__DORIS_DELETE_SIGN__ is a hidden column in doris that indicates whether the data of the row should be deleted or not," +
                    "in cdc, __DORIS_DELETE_SIGN__ is true to indicate that the row should be deleted or false to indicate insertion. " +
                    "See the Apache Doris documentation for more details")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("__DORIS_DELETE_SIGN__")
            .build();

    public static final PropertyDescriptor OP = new PropertyDescriptor
            .Builder().name("Op Key")
            .description("The key in json data, the default value is op. If you want to replace it with another field, you can change this value:\n" +
                    "The PutDoris processor reads the value of op in the data and uses it to determine __DORIS_DELETE_SIGN__; " +
                    "if op is delete, __DORIS_DELETE_SIGN__=true; otherwise, __DORIS_DELETE_SIGN__ is false")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("op")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to Apache Doirs are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to Apache Doirs for some reason are transferred to this relationship")
            .build();

    public static final PropertyDescriptor ONETOONE = new PropertyDescriptor
            .Builder().name("One To One")
            .description("one-to-one means where did the data come from and be written to Apache Doris in which " +
                    "database and which form (sample srcDatabase. SrcTable: destDatabase destTable, srcDatabase. SrcTable1: destDatabase. DestTable1, foo...):\n" +
                    "eg:\n" +
                    "(Assume SRC_DATABASE_NAME_KEY=database and SRC_TABLE_NAME_KEY=table_name)\n" +
                    "There are three json pieces of data in the queue\n" +
                    "data1 -&gt;  {\"database\":\"db1\",\"table_name\":\"tab1\",\"field1\":\"a\",\"field2\":\"b\"}\n" +
                    "data2 -&gt;  {\"database\":\"db1\",\"table_name\":\"tab2\",\"field1\":\"a\",\"field2\":\"b\"}\n" +
                    "data3 -&gt;  {\"database\":\"db2\",\"table_name\":\"tab1\",\"field1\":\"a\",\"field2\":\"b\"}\n" +
                    "\n" +
                    "1. If not configured, SRC_DATABASE_NAME_KEY and SRC_TABLE_NAME_KEY will be used as the database and table to be written to\n" +
                    "Result: data1 will be written to doris with database db1 and table tab1\n" +
                    "        data2 is written to doris, whose database is db1 and whose table is tab2\n" +
                    "        data3 is written to doris with db2 as database and tab1 as table\n" +
                    "If configured, db1.tab1:db1_doris.tab_doris1,db1.tab2:db1_doris.tab_doris1,db2.tab1:db1_doris.tab_doris1\n" +
                    "Result: data1 is written to doris with db1_doris as database and tab_doris1 as table\n" +
                    "        data2 is written to doris with db1_doris as the database and tab_doris1 as the table\n" +
                    "        data3 is written to doris with db1_doris as the database and tab_doris1 as the table\n" +
                    "Note: If you configure ONETOONE, you need to configure all the data relationships to be synchronized in ONETOONE to prevent data confusion")
            .required(false)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("(\\w+\\.\\w+:\\w+\\.\\w+)(,\\w+\\.\\w+:\\w+\\.\\w+)*")))
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of FlowFiles to process in a single execution, between 1 - 100000. " +
                    "Depending on your memory size, and data size per row set an appropriate batch size " +
                    "for the number of FlowFiles to process per client connection setup." +
                    "Gradually increase this number, only if your FlowFiles typically contain a few records.")
            .defaultValue("1")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 100000, true))
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
                        rootNodeRef.set(jnList);
                    }

                } catch (final ProcessException pe) {
                    getLogger().error("failed to process session due to {}", new Object[]{flowFile, pe});
                    session.transfer(flowFile, REL_FAILURE);
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
                    getLogger().error("dorisFlowFile is null {}", new Object[]{dorisFlowFile});
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

            }

        }
        //Delete the original flowfile
        session.remove(flowFiles);

        //Write the data to doris
        for (Map.Entry<String, List<String>> stringListEntry : putJsonMap.entrySet()) {

            String key = stringListEntry.getKey();
            String destDatabase = key.split("\\.", -1)[0];
            String destTableName = key.split("\\.", -1)[1];
            List<String> value = putJsonMap.get(key);
            String join = String.join("\n", value);
            FlowFile putFlowFile = session.create();
            session.write(putFlowFile, outputStream -> outputStream.write(join.getBytes()));

            try {
                Result result = dorisClientService.putJson(stringListEntry.getValue().toString(), destDatabase, destTableName);
                getLogger().info(result.getLoadResult().toString());
                if (result.getStatusCode() != 200) {
                    throw new RuntimeException(String.format("Doris Stream load failed. status: %s load result: %s", result.getStatusCode(), result.getLoadResult()));
                }
                if (!"Success".equalsIgnoreCase(mapper.readTree(result.getLoadResult()).get("Status").asText())) {
                    throw new RuntimeException(String.format("Doris Stream load failed. status: %s load Message: %s",
                            result.getStatusCode(), mapper.readTree(result.getLoadResult()).get("Message").asText()));
                }

                session.transfer(putFlowFile, REL_SUCCESS);
            } catch (Exception e) {
                getLogger().error(e.toString());
                session.transfer(putFlowFile, REL_FAILURE);
            }
        }
    }
}
