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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.solr.SolrUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SolrClientServiceIT {
    private TestRunner runner;
    private SolrClientService clientService;

    @Before
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(TestProcessor.class);
        clientService = new SolrClientServiceImpl();
        runner.addControllerService(SolrUtils.CLIENT_SERVICE.getName(), clientService);
    }

    /*
     * Setup:
     *
     * bin/solr start -p 8985 -e techproducts
     */

    @Test
    public void testSolrHttp() throws Exception {
        runner.setProperty(clientService, SolrUtils.SOLR_LOCATION, "http://localhost:8985/solr/techproducts");
        runner.enableControllerService(clientService);
        runner.assertValid();

        SolrClient client = clientService.getClient();

        count(client);
    }

    /*
     * Setup:
     *
     * bin/solr start -e cloud
     *
     * Set collection name to gettingstarted (should be default)
     * Set config to the "techproducts" config when it asks for that or _default
     */
    @Test
    public void testSolrCloud() throws Exception {
        runner.setProperty(clientService, SolrUtils.SOLR_TYPE, SolrUtils.SOLR_TYPE_CLOUD);
        runner.setProperty(clientService, SolrUtils.SOLR_LOCATION, "localhost:9983");
        runner.setProperty(clientService, SolrUtils.COLLECTION, "gettingstarted");
        runner.enableControllerService(clientService);
        runner.assertValid();

        SolrClient client = clientService.getClient();
        count(client);
    }

    private void count(SolrClient client) throws Exception {
        SolrQuery query = new SolrQuery();
        query.setQuery("cat:memory");
        query.setRows(0);
        long count = client.query(query).getResults().getNumFound();
        Assert.assertEquals("Should have been only 1", 3, count);
    }

    public static class TestProcessor extends AbstractProcessor {
        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return Arrays.asList(SolrUtils.CLIENT_SERVICE);
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        }
    }
}
