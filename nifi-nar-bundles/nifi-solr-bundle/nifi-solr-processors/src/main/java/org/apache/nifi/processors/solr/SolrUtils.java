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
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SolrUtils {

    final PropertyDescriptor SOLR_SOCKET_TIMEOUT;
    final PropertyDescriptor SOLR_CONNECTION_TIMEOUT;
    final PropertyDescriptor SOLR_MAX_CONNECTIONS;
    final PropertyDescriptor SOLR_MAX_CONNECTIONS_PER_HOST;
    final PropertyDescriptor SSL_CONTEXT_SERVICE;
    final PropertyDescriptor JAAS_CLIENT_APP_NAME;
    final PropertyDescriptor SOLR_TYPE;
    final PropertyDescriptor COLLECTION;
    final PropertyDescriptor ZK_CLIENT_TIMEOUT;
    final PropertyDescriptor ZK_CONNECTION_TIMEOUT;
    final AllowableValue SOLR_TYPE_STANDARD;

    public SolrUtils(SolrProcessor solrProcessor) {
        SOLR_SOCKET_TIMEOUT = SolrProcessor.SOLR_SOCKET_TIMEOUT;
        SOLR_CONNECTION_TIMEOUT = solrProcessor.SOLR_CONNECTION_TIMEOUT;
        SOLR_MAX_CONNECTIONS = solrProcessor.SOLR_MAX_CONNECTIONS;
        SOLR_MAX_CONNECTIONS_PER_HOST = solrProcessor.SOLR_MAX_CONNECTIONS_PER_HOST;
        SSL_CONTEXT_SERVICE = solrProcessor.SSL_CONTEXT_SERVICE;
        JAAS_CLIENT_APP_NAME = solrProcessor.JAAS_CLIENT_APP_NAME;
        SOLR_TYPE = solrProcessor.SOLR_TYPE;
        COLLECTION = solrProcessor.COLLECTION;
        ZK_CLIENT_TIMEOUT = solrProcessor.ZK_CLIENT_TIMEOUT;
        ZK_CONNECTION_TIMEOUT = solrProcessor.ZK_CONNECTION_TIMEOUT;
        SOLR_TYPE_STANDARD = solrProcessor.SOLR_TYPE_STANDARD;

    }

    /*
    public SolrUtils(SolrControllerService solrControllerService) {
        SOLR_SOCKET_TIMEOUT = solrControllerService.SOLR_SOCKET_TIMEOUT;
        ...
    }
    */


    protected SolrClient createSolrClient(final PropertyContext context, final String solrLocation) {
        final Integer socketTimeout = context.getProperty(SOLR_SOCKET_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final Integer connectionTimeout = context.getProperty(SOLR_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final Integer maxConnections = context.getProperty(SOLR_MAX_CONNECTIONS).asInteger();
        final Integer maxConnectionsPerHost = context.getProperty(SOLR_MAX_CONNECTIONS_PER_HOST).asInteger();
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final String jaasClientAppName = context.getProperty(JAAS_CLIENT_APP_NAME).getValue();

        final ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(HttpClientUtil.PROP_SO_TIMEOUT, socketTimeout);
        params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, connectionTimeout);
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, maxConnections);
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);

        // has to happen before the client is created below so that correct configurer would be set if neeeded
        if (!StringUtils.isEmpty(jaasClientAppName)) {
            System.setProperty("solr.kerberos.jaas.appname", jaasClientAppName);
            HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
        }

        final HttpClient httpClient = HttpClientUtil.createClient(params);

        if (sslContextService != null) {
            final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
            final SSLSocketFactory sslSocketFactory = new SSLSocketFactory(sslContext);
            final Scheme httpsScheme = new Scheme("https", 443, sslSocketFactory);
            httpClient.getConnectionManager().getSchemeRegistry().register(httpsScheme);
        }

        if (SOLR_TYPE_STANDARD.equals(context.getProperty(SOLR_TYPE).getValue())) {
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


}
