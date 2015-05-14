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

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Helper to create EmbeddedSolrServer instances for testing.
 */
public class EmbeddedSolrServerFactory {

    public static final String DEFAULT_SOLR_HOME = "src/test/resources/solr";
    public static final String DEFAULT_CORE_HOME = "src/test/resources/";
    public static final String DEFAULT_DATA_DIR = "target";

    /**
     * Use the defaults to create the core.
     *
     * @param coreName the name of the core
     * @return an EmbeddedSolrServer for the given core
     */
    public static SolrClient create(String coreName) throws IOException {
        return create(DEFAULT_SOLR_HOME, DEFAULT_CORE_HOME,
                coreName, DEFAULT_DATA_DIR);
    }

    /**
     *
     * @param solrHome
     *              path to directory where solr.xml lives
     * @param coreName
     *              the name of the core to load
     * @param dataDir
     *              the data dir for the core
     *
     * @return an EmbeddedSolrServer for the given core
     */
    public static SolrClient create(String solrHome, String coreHome, String coreName, String dataDir)
            throws IOException {

        Properties props = new Properties();
        if (dataDir != null) {
            File coreDataDir = new File(dataDir + "/" + coreName);
            if (coreDataDir.exists()) {
                FileUtils.deleteDirectory(coreDataDir);
            }
            props.setProperty("dataDir", dataDir + "/" + coreName);
        }

        CoreContainer coreContainer = new CoreContainer(solrHome);
        coreContainer.load();

        CoreDescriptor descriptor = new CoreDescriptor(coreContainer, coreName,
                new File(coreHome, coreName).getAbsolutePath(), props);

        coreContainer.create(descriptor);
        return new EmbeddedSolrServer(coreContainer, coreName);
    }
}


