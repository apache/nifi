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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpPut;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.doris.DorisClientService;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.StringUtils;

import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"doris"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class MyProcessor extends AbstractProcessor {
    public static final PropertyDescriptor DORIS_CLIENT_SERVICE = new PropertyDescriptor
            .Builder().name("DORIS_CLIENT_SERVICE")
            .displayName("doris_client_service")
            .description("DORIS_CLIENT_SERVICE")
            .required(true)
            .identifiesControllerService(DorisClientService.class)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are sent to the AMQP destination are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be routed to the AMQP destination are routed to this relationship")
            .build();

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();


    public static final PropertyDescriptor SRC_DATABASE = new PropertyDescriptor
            .Builder().name("SRC_DATABASE")
            .displayName("src_database")
            .description("SRC_DATABASE")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    /*public static final PropertyDescriptor SRC_TABLE = new PropertyDescriptor
            .Builder().name("SRC_TABLE")
            .displayName("src_tablename")
            .description("SRC_TABLE")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DEST_DATABASE = new PropertyDescriptor
            .Builder().name("DEST_DATABASE")
            .displayName("dest_database")
            .description("DEST_DATABASE")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DEST_TABLE = new PropertyDescriptor
            .Builder().name("DEST_TABLE")
            .displayName("dest_tablename")
            .description("DEST_TABLE")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();*/

    public static final PropertyDescriptor ONETOONE = new PropertyDescriptor
            .Builder().name("one-to-one")
            .displayName("one-to-one")
            .description("one-to-one")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    /*public static final PropertyDescriptor COLUMNS = new PropertyDescriptor
            .Builder().name("COLUMNS")
            .displayName("columns")
            .description("COLUMNS")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("-1")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();*/
    public static final PropertyDescriptor LABEL = new PropertyDescriptor
            .Builder().name("LABEL")
            .displayName("label")
            .description("LABEL")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("label-" + UUID.randomUUID())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(DORIS_CLIENT_SERVICE);
        descriptors.add(RECORD_READER_FACTORY);
        /*descriptors.add(SRC_DATABASE);
        descriptors.add(SRC_TABLE);
        descriptors.add(DEST_DATABASE);
        descriptors.add(DEST_TABLE);*/
        descriptors.add(ONETOONE);
        descriptors.add(LABEL);
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

    protected DorisClientService dorisClientService;

    protected HashMap<String, String> oneToOneMap = new HashMap<>();

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        dorisClientService = context.getProperty(DORIS_CLIENT_SERVICE).asControllerService(DorisClientService.class);
        final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);

        ArrayList<String> flowFiles = new ArrayList<>();

        try (final InputStream in = session.read(flowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(flowFile, in, getLogger())) {
            Record record;
            final ObjectMapper mapper = new ObjectMapper();

            while ((record = reader.nextRecord()) != null) {

                String srcDatabase = flowFile.getAttribute("src_database");
                String srcTable = flowFile.getAttribute("src_table");
                String columns = flowFile.getAttribute("columns");
                String op = flowFile.getAttribute("cdc.event.type");
                String srcUnionAndDestUnion = srcDatabase + "." + srcTable;

                if (null == columns) {
                    throw new Exception("dd");
                } else {
                    columns = columns.concat(",`__DORIS_DELETE_SIGN__`");
                }

                if (null == op) {
                    throw new Exception("bb");
                }


                if (context.getProperty(ONETOONE).isSet()) {
                    //srcDatabase.srcTable:destDatabase.destTable,srcDatabase.srcTable1:destDatabase.destTable1,foo...
                    String oneToOneValue = context.getProperty(ONETOONE).evaluateAttributeExpressions().getValue();
                    String[] split = oneToOneValue.split(",", -1);
                    for (String s : split) {
                        String[] databaseAndTable = s.split(":", -1);
                        oneToOneMap.put(databaseAndTable[0], databaseAndTable[1]);
                    }

                } else {
                    //src=dest
                    oneToOneMap.put(srcUnionAndDestUnion, srcUnionAndDestUnion);
                }

                String[] split = oneToOneMap.get(srcUnionAndDestUnion).split(".", -1);


                //init client
                HashMap<String, HttpPut> stringHttpPutHashMap = dorisClientService.setClient(split[0], split[1], columns, context.getProperty(LABEL).evaluateAttributeExpressions().getValue());

                //getLogger().error("--> stringHttpPutHashMap.size:{} <--", stringHttpPutHashMap.size());

                dorisClientService.insert(record.toString(), split[0], split[1]);

                /*if (context.getProperty(SRC_DATABASE).isSet()) {
                    srcDatabase = context.getProperty(SRC_DATABASE).evaluateAttributeExpressions().getValue();
                }
                if (context.getProperty(SRC_TABLE).isSet()) {
                    srcTable = context.getProperty(SRC_TABLE).evaluateAttributeExpressions().getValue();
                }

                if (context.getProperty(DEST_DATABASE).isSet()) {
                    destDatabase = context.getProperty(DEST_DATABASE).evaluateAttributeExpressions().getValue();
                }
                if (context.getProperty(DEST_TABLE).isSet()) {
                    destTable = context.getProperty(DEST_TABLE).evaluateAttributeExpressions().getValue();
                }*/


                if (reader != null) {


                }


            }


        } catch (Exception e) {
            getLogger().error("Failed to put records to Doris.", e);
        }


        session.transfer(flowFile, REL_SUCCESS);


    }
}
