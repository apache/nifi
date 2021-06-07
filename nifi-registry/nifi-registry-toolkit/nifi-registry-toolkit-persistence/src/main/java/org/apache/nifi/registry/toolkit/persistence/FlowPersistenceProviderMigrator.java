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
package org.apache.nifi.registry.toolkit.persistence;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.nifi.registry.NiFiRegistry;
import org.apache.nifi.registry.db.DataSourceFactory;
import org.apache.nifi.registry.db.DatabaseMetadataService;
import org.apache.nifi.registry.db.entity.BucketEntity;
import org.apache.nifi.registry.db.entity.FlowEntity;
import org.apache.nifi.registry.db.entity.FlowSnapshotEntity;
import org.apache.nifi.registry.extension.ExtensionManager;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.provider.StandardProviderFactory;
import org.apache.nifi.registry.provider.flow.StandardFlowSnapshotContext;
import org.apache.nifi.registry.service.MetadataService;
import org.apache.nifi.registry.service.mapper.BucketMappings;
import org.apache.nifi.registry.service.mapper.FlowMappings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Properties;

public class FlowPersistenceProviderMigrator {
    private static final Logger log = LoggerFactory.getLogger(FlowPersistenceProviderMigrator.class);
    public static final int PARSE_EXCEPTION = 1;

    public void doMigrate(MetadataService fromMetadata, FlowPersistenceProvider fromProvider, FlowPersistenceProvider toProvider) {
        for (BucketEntity bucket : fromMetadata.getAllBuckets()) {
            for (FlowEntity flow : fromMetadata.getFlowsByBucket(bucket.getId())) {
                for (FlowSnapshotEntity flowSnapshot : fromMetadata.getSnapshots(flow.getId())) {
                    StandardFlowSnapshotContext context = new StandardFlowSnapshotContext.Builder(
                            BucketMappings.map(bucket),
                            FlowMappings.map(bucket, flow),
                            FlowMappings.map(bucket, flowSnapshot)).build();

                    int version = flowSnapshot.getVersion();

                    toProvider.saveFlowContent(context, fromProvider.getFlowContent(bucket.getId(), flow.getId(), version));

                    log.info("Migrated flow {} version {}", flow.getName(), version);
                }
            }
        }
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("t", "to", true, "Providers xml to migrate to.");
        CommandLineParser parser = new DefaultParser();

        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Unable to parse command line.", e);

            new HelpFormatter().printHelp("persistence-toolkit [args]", options);

            System.exit(PARSE_EXCEPTION);
        }

        NiFiRegistryProperties fromProperties = NiFiRegistry.initializeProperties(NiFiRegistry.getMasterKeyProvider());

        DataSource dataSource = new DataSourceFactory(fromProperties).getDataSource();
        DatabaseMetadataService fromMetadataService = new DatabaseMetadataService(new JdbcTemplate(dataSource));
        FlowPersistenceProvider fromPersistenceProvider = createFlowPersistenceProvider(fromProperties, dataSource);
        FlowPersistenceProvider toPersistenceProvider = createFlowPersistenceProvider(createToProperties(commandLine, fromProperties), dataSource);

        new FlowPersistenceProviderMigrator().doMigrate(fromMetadataService, fromPersistenceProvider, toPersistenceProvider);
    }

    private static NiFiRegistryProperties createToProperties(final CommandLine commandLine, final NiFiRegistryProperties fromProperties) {
        final Properties props = new Properties();
        for (final String propertyKey : fromProperties.getPropertyKeys()) {
            props.setProperty(propertyKey, fromProperties.getProperty(propertyKey));
        }
        props.setProperty(NiFiRegistryProperties.PROVIDERS_CONFIGURATION_FILE, commandLine.getOptionValue('t'));
        final NiFiRegistryProperties toProperties = new NiFiRegistryProperties(props);
        return toProperties;
    }

    private static FlowPersistenceProvider createFlowPersistenceProvider(NiFiRegistryProperties niFiRegistryProperties, DataSource dataSource) {
        ExtensionManager fromExtensionManager = new ExtensionManager(niFiRegistryProperties);
        fromExtensionManager.discoverExtensions();
        StandardProviderFactory fromProviderFactory = new StandardProviderFactory(niFiRegistryProperties, fromExtensionManager, dataSource);
        fromProviderFactory.initialize();
        return fromProviderFactory.getFlowPersistenceProvider();
    }
}