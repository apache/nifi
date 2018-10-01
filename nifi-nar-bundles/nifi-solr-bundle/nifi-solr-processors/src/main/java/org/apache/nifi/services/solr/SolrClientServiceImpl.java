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

package org.apache.nifi.services.solr;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.solr.client.solrj.SolrClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.nifi.processors.solr.SolrUtils.BASIC_PASSWORD;
import static org.apache.nifi.processors.solr.SolrUtils.BASIC_USERNAME;
import static org.apache.nifi.processors.solr.SolrUtils.COLLECTION;
import static org.apache.nifi.processors.solr.SolrUtils.KERBEROS_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_CONNECTION_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_LOCATION;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_MAX_CONNECTIONS;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_MAX_CONNECTIONS_PER_HOST;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_SOCKET_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_TYPE;
import static org.apache.nifi.processors.solr.SolrUtils.SSL_CONTEXT_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.ZK_CLIENT_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.ZK_CONNECTION_TIMEOUT;
import static org.apache.nifi.processors.solr.SolrUtils.createSolrClient;

public class SolrClientServiceImpl extends AbstractControllerService implements SolrClientService {
    private static final List<PropertyDescriptor> DESCRIPTORS;

    static PropertyDescriptor makeRequired(PropertyDescriptor _desc) {
        return new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(_desc)
            .required(true)
            .build();
    }

    static {
        List<PropertyDescriptor> _temp = new ArrayList<>();

        _temp.add(makeRequired(SOLR_TYPE));
        _temp.add(makeRequired(SOLR_LOCATION));
        _temp.add(COLLECTION);
        _temp.add(BASIC_USERNAME);
        _temp.add(BASIC_PASSWORD);
        _temp.add(KERBEROS_CREDENTIALS_SERVICE);
        _temp.add(SSL_CONTEXT_SERVICE);
        _temp.add(makeRequired(SOLR_SOCKET_TIMEOUT));
        _temp.add(makeRequired(SOLR_CONNECTION_TIMEOUT));
        _temp.add(makeRequired(SOLR_MAX_CONNECTIONS));
        _temp.add(makeRequired(SOLR_MAX_CONNECTIONS_PER_HOST));
        _temp.add(ZK_CLIENT_TIMEOUT);
        _temp.add(ZK_CONNECTION_TIMEOUT);

        DESCRIPTORS = Collections.unmodifiableList(_temp);
    }

    private volatile SolrClient solrClient;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        String location = context.getProperty(SOLR_LOCATION).evaluateAttributeExpressions().getValue();
        solrClient = createSolrClient(context, location);
    }

    @Override
    public SolrClient getClient() {
        return solrClient;
    }
}
