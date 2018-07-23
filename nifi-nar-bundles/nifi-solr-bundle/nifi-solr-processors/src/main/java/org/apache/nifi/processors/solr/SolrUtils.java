/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.solr;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class SolrUtils {

    static final Logger LOGGER = LoggerFactory.getLogger(SolrUtils.class);

    public static final AllowableValue SOLR_TYPE_CLOUD = new AllowableValue(
            "Cloud", "Cloud", "A SolrCloud instance.");

    public static final AllowableValue SOLR_TYPE_STANDARD = new AllowableValue(
            "Standard", "Standard", "A stand-alone Solr instance.");

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor
            .Builder().name("Record Writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to write Solr documents to FlowFiles. Must be set if \"Records\" is used as return type.")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor SOLR_TYPE = new PropertyDescriptor
            .Builder().name("Solr Type")
            .description("The type of Solr instance, Cloud or Standard.")
            .required(true)
            .allowableValues(SOLR_TYPE_CLOUD, SOLR_TYPE_STANDARD)
            .defaultValue(SOLR_TYPE_STANDARD.getValue())
            .build();

    public static final PropertyDescriptor COLLECTION = new PropertyDescriptor
            .Builder().name("Collection")
            .description("The Solr collection name, only used with a Solr Type of Cloud")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SOLR_LOCATION = new PropertyDescriptor
            .Builder().name("Solr Location")
            .description("The Solr url for a Solr Type of Standard (ex: http://localhost:8984/solr/gettingstarted), " +
                    "or the ZooKeeper hosts for a Solr Type of Cloud (ex: localhost:9983).")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor BASIC_USERNAME = new PropertyDescriptor
            .Builder().name("Username")
            .description("The username to use when Solr is configured with basic authentication.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor BASIC_PASSWORD = new PropertyDescriptor
            .Builder().name("Password")
            .description("The password to use when Solr is configured with basic authentication.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(true)
            .build();

    static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosCredentialsService.class)
            .required(false)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. This property must be set when communicating with a Solr over https.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor SOLR_SOCKET_TIMEOUT = new PropertyDescriptor
            .Builder().name("Solr Socket Timeout")
            .description("The amount of time to wait for data on a socket connection to Solr. A value of 0 indicates an infinite timeout.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .build();

    public static final PropertyDescriptor SOLR_CONNECTION_TIMEOUT = new PropertyDescriptor
            .Builder().name("Solr Connection Timeout")
            .description("The amount of time to wait when establishing a connection to Solr. A value of 0 indicates an infinite timeout.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .build();

    public static final PropertyDescriptor SOLR_MAX_CONNECTIONS = new PropertyDescriptor
            .Builder().name("Solr Maximum Connections")
            .description("The maximum number of total connections allowed from the Solr client to Solr.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    public static final PropertyDescriptor SOLR_MAX_CONNECTIONS_PER_HOST = new PropertyDescriptor
            .Builder().name("Solr Maximum Connections Per Host")
            .description("The maximum number of connections allowed from the Solr client to a single Solr host.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    public static final PropertyDescriptor ZK_CLIENT_TIMEOUT = new PropertyDescriptor
            .Builder().name("ZooKeeper Client Timeout")
            .description("The amount of time to wait for data on a connection to ZooKeeper, only used with a Solr Type of Cloud.")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .defaultValue("10 seconds")
            .build();

    public static final PropertyDescriptor ZK_CONNECTION_TIMEOUT = new PropertyDescriptor
            .Builder().name("ZooKeeper Connection Timeout")
            .description("The amount of time to wait when establishing a connection to ZooKeeper, only used with a Solr Type of Cloud.")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .defaultValue("10 seconds")
            .build();

    public static final String REPEATING_PARAM_PATTERN = "[\\w\\.]+\\.\\d+$";

    public static SolrClient createSolrClient(final PropertyContext context, final String solrLocation) {
        final Integer socketTimeout = context.getProperty(SOLR_SOCKET_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final Integer connectionTimeout = context.getProperty(SOLR_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final Integer maxConnections = context.getProperty(SOLR_MAX_CONNECTIONS).asInteger();
        final Integer maxConnectionsPerHost = context.getProperty(SOLR_MAX_CONNECTIONS_PER_HOST).asInteger();
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final KerberosCredentialsService kerberosCredentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        final ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(HttpClientUtil.PROP_SO_TIMEOUT, socketTimeout);
        params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, connectionTimeout);
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, maxConnections);
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);

        // has to happen before the client is created below so that correct configurer would be set if needed
        if (kerberosCredentialsService != null) {
            HttpClientUtil.setConfigurer(new KerberosHttpClientConfigurer());
        }

        final HttpClient httpClient = HttpClientUtil.createClient(params);

        if (sslContextService != null) {
            final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
            final SSLSocketFactory sslSocketFactory = new SSLSocketFactory(sslContext);
            final Scheme httpsScheme = new Scheme("https", 443, sslSocketFactory);
            httpClient.getConnectionManager().getSchemeRegistry().register(httpsScheme);
        }

        if (SOLR_TYPE_STANDARD.getValue().equals(context.getProperty(SOLR_TYPE).getValue())) {
            return new HttpSolrClient(solrLocation, httpClient);
        } else {
            final String collection = context.getProperty(COLLECTION).evaluateAttributeExpressions().getValue();
            final Integer zkClientTimeout = context.getProperty(ZK_CLIENT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            final Integer zkConnectionTimeout = context.getProperty(ZK_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

            CloudSolrClient cloudSolrClient = new CloudSolrClient(solrLocation, httpClient);
            cloudSolrClient.setDefaultCollection(collection);
            cloudSolrClient.setZkClientTimeout(zkClientTimeout);
            cloudSolrClient.setZkConnectTimeout(zkConnectionTimeout);
            return cloudSolrClient;
        }
    }

    /**
     * Writes each SolrDocument to a record.
     */
    public static RecordSet solrDocumentsToRecordSet(final List<SolrDocument> docs, final RecordSchema schema) {
        final List<Record> lr = new ArrayList<Record>();

        for (SolrDocument doc : docs) {
            final Map<String, Object> recordValues = new LinkedHashMap<>();
            for (RecordField field : schema.getFields()){
                final Object fieldValue = doc.getFieldValue(field.getFieldName());
                if (fieldValue != null) {
                    if (field.getDataType().getFieldType().equals(RecordFieldType.ARRAY)){
                        recordValues.put(field.getFieldName(), ((List<Object>) fieldValue).toArray());
                    } else {
                        recordValues.put(field.getFieldName(), fieldValue);
                    }
                }
            }
            lr.add(new MapRecord(schema, recordValues));
        }
        return new ListRecordSet(schema, lr);
    }

    public static OutputStreamCallback getOutputStreamCallbackToTransformSolrResponseToXml(QueryResponse response) {
        return new QueryResponseOutputStreamCallback(response);
    }

    /**
     * Writes each SolrDocument in XML format to the OutputStream.
     */
    private static class QueryResponseOutputStreamCallback implements OutputStreamCallback {
        private QueryResponse response;

        public QueryResponseOutputStreamCallback(QueryResponse response) {
            this.response = response;
        }

        @Override
        public void process(OutputStream out) throws IOException {
            IOUtils.write("<docs>", out, StandardCharsets.UTF_8);
            for (SolrDocument doc : response.getResults()) {
                final String xml = ClientUtils.toXML(toSolrInputDocument(doc));
                IOUtils.write(xml, out, StandardCharsets.UTF_8);
            }
            IOUtils.write("</docs>", out, StandardCharsets.UTF_8);
        }

        public SolrInputDocument toSolrInputDocument(SolrDocument d) {
            final SolrInputDocument doc = new SolrInputDocument();

            for (String name : d.getFieldNames()) {
                doc.addField(name, d.getFieldValue(name));
            }

            return doc;
        }
    }

    public static Map<String, String[]> getRequestParams(ProcessContext context, FlowFile flowFile) {
        final Map<String,String[]> paramsMap = new HashMap<>();
        final SortedMap<String,String> repeatingParams = new TreeMap<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                final String paramName = descriptor.getName();
                final String paramValue = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();

                if (!paramValue.trim().isEmpty()) {
                    if (paramName.matches(REPEATING_PARAM_PATTERN)) {
                        repeatingParams.put(paramName, paramValue);
                    } else {
                        MultiMapSolrParams.addParam(paramName, paramValue, paramsMap);
                    }
                }
            }
        }

        for (final Map.Entry<String,String> entry : repeatingParams.entrySet()) {
            final String paramName = entry.getKey();
            final String paramValue = entry.getValue();
            final int idx = paramName.lastIndexOf(".");
            MultiMapSolrParams.addParam(paramName.substring(0, idx), paramValue, paramsMap);
        }

        return paramsMap;
    }

    /**
     * Writes each Record as a SolrInputDocument.
     */
    public static void writeRecord(final Record record, final SolrInputDocument inputDocument,final List<String> fieldsToIndex,String parentFieldName)
            throws IOException {
        RecordSchema schema = record.getSchema();

        for (int i = 0; i < schema.getFieldCount(); i++) {
            final RecordField field = schema.getField(i);
            String fieldName;
            if(!StringUtils.isBlank(parentFieldName)) {
                // Prefixing parent field name
                fieldName = parentFieldName+"_"+field.getFieldName();
            }else{
                fieldName = field.getFieldName();
            }
            final Object value = record.getValue(field);
            if (value == null) {
                continue;
            }else {
                final DataType dataType = schema.getDataType(field.getFieldName()).get();
                writeValue(inputDocument, value, fieldName, dataType,fieldsToIndex);
            }
        }
    }

    private static void writeValue(final SolrInputDocument inputDocument, final Object value, final String fieldName, final DataType dataType,final List<String> fieldsToIndex) throws IOException {
        final DataType chosenDataType = dataType.getFieldType() == RecordFieldType.CHOICE ? DataTypeUtils.chooseDataType(value, (ChoiceDataType) dataType) : dataType;
        final Object coercedValue = DataTypeUtils.convertType(value, chosenDataType, fieldName);
        if (coercedValue == null) {
            return;
        }

        switch (chosenDataType.getFieldType()) {
            case DATE: {
                final String stringValue = DataTypeUtils.toString(coercedValue, () -> DataTypeUtils.getDateFormat(RecordFieldType.DATE.getDefaultFormat()));
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    LocalDate localDate = getLocalDateFromEpochTime(fieldName, coercedValue);
                    addFieldToSolrDocument(inputDocument,fieldName,localDate.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)+'Z',fieldsToIndex);
                } else {
                    addFieldToSolrDocument(inputDocument,fieldName,LocalDate.parse(stringValue).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)+'Z',fieldsToIndex);
                }
                break;
            }
            case TIMESTAMP: {
                final String stringValue = DataTypeUtils.toString(coercedValue, () -> DataTypeUtils.getDateFormat(RecordFieldType.TIMESTAMP.getDefaultFormat()));
                if (DataTypeUtils.isLongTypeCompatible(stringValue)) {
                    LocalDateTime localDateTime = getLocalDateTimeFromEpochTime(fieldName, coercedValue);
                    addFieldToSolrDocument(inputDocument,fieldName,localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)+'Z',fieldsToIndex);
                } else {
                    addFieldToSolrDocument(inputDocument,fieldName,LocalDateTime.parse(stringValue).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)+'Z',fieldsToIndex);
                }
                break;
            }
            case DOUBLE:
                addFieldToSolrDocument(inputDocument,fieldName,DataTypeUtils.toDouble(coercedValue, fieldName),fieldsToIndex);
                break;
            case FLOAT:
                addFieldToSolrDocument(inputDocument,fieldName,DataTypeUtils.toFloat(coercedValue, fieldName),fieldsToIndex);
                break;
            case LONG:
                addFieldToSolrDocument(inputDocument,fieldName,DataTypeUtils.toLong(coercedValue, fieldName),fieldsToIndex);
                break;
            case INT:
            case BYTE:
            case SHORT:
                addFieldToSolrDocument(inputDocument,fieldName,DataTypeUtils.toInteger(coercedValue, fieldName),fieldsToIndex);
                break;
            case CHAR:
            case STRING:
                addFieldToSolrDocument(inputDocument,fieldName,coercedValue.toString(),fieldsToIndex);
                break;
            case BIGINT:
                if (coercedValue instanceof Long) {
                    addFieldToSolrDocument(inputDocument,fieldName,(Long) coercedValue,fieldsToIndex);
                } else {
                    addFieldToSolrDocument(inputDocument,fieldName,(BigInteger)coercedValue,fieldsToIndex);
                }
                break;
            case BOOLEAN:
                final String stringValue = coercedValue.toString();
                if ("true".equalsIgnoreCase(stringValue)) {
                    addFieldToSolrDocument(inputDocument,fieldName,true,fieldsToIndex);
                } else if ("false".equalsIgnoreCase(stringValue)) {
                    addFieldToSolrDocument(inputDocument,fieldName,false,fieldsToIndex);
                } else {
                    addFieldToSolrDocument(inputDocument,fieldName,stringValue,fieldsToIndex);
                }
                break;
            case RECORD: {
                final Record record = (Record) coercedValue;
                writeRecord(record, inputDocument,fieldsToIndex,fieldName);
                break;
            }
            case ARRAY:
            default:
                if (coercedValue instanceof Object[]) {
                    final Object[] values = (Object[]) coercedValue;
                    for(Object element : values){
                        if(element instanceof  Record){
                            writeRecord((Record)element,inputDocument,fieldsToIndex,fieldName);
                        }else{
                            addFieldToSolrDocument(inputDocument,fieldName,coercedValue.toString(),fieldsToIndex);
                        }
                    }
                } else {
                    addFieldToSolrDocument(inputDocument,fieldName,coercedValue.toString(),fieldsToIndex);
                }
                break;
        }
    }

    private static void addFieldToSolrDocument(SolrInputDocument inputDocument,String fieldName,Object fieldValue,List<String> fieldsToIndex){
        if ((!fieldsToIndex.isEmpty() && fieldsToIndex.contains(fieldName)) || fieldsToIndex.isEmpty()){
            inputDocument.addField(fieldName, fieldValue);
        }
    }

    private static LocalDate getLocalDateFromEpochTime(String fieldName, Object coercedValue) {
        Long date = DataTypeUtils.toLong(coercedValue, fieldName);
        return Instant.ofEpochMilli(date).atZone(ZoneId.systemDefault()).toLocalDate();
    }

    private static LocalDateTime getLocalDateTimeFromEpochTime(String fieldName, Object coercedValue) {
        Long date = DataTypeUtils.toLong(coercedValue, fieldName);
        return Instant.ofEpochMilli(date).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

}
