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
package org.apache.nifi.processors.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventRequestData;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.util.hive.AuthenticationFailedException;
import org.apache.nifi.util.hive.HiveConfigurator;
import org.apache.nifi.util.hive.ValidationResources;
import org.apache.thrift.TException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@DeprecationNotice(reason = "Support for Apache Hive 3 is deprecated for removal in Apache NiFi 2.0")
@Tags({"hive", "metastore", "notification", "insert", "delete", "partition", "event"})
@CapabilityDescription("The processor is capable to trigger different type of events in the HiveMetaStore and generate notifications. " +
        "The metastore action to be executed is determined from the incoming path and event type attributes. " +
        "The supported event type values are 'put' in case of data insertion or 'delete' in case of data removal. " +
        "The notifications should be enabled in the metastore configuration to generate them e.g.: the 'hive.metastore.transactional.event.listeners' " +
        "should have a proper listener configured, for instance 'org.apache.hive.hcatalog.listener.DbNotificationListener'.")
@WritesAttributes({
        @WritesAttribute(attribute = "metastore.notification.event", description = "The event type of the triggered notification.")
})
public class TriggerHiveMetaStoreEvent extends AbstractProcessor {

    public static final String METASTORE_NOTIFICATION_EVENT = "metastore.notification.event";

    private final HiveConfigurator hiveConfigurator = new HiveConfigurator();

    private Configuration hiveConfig;

    private final AtomicReference<KerberosUser> kerberosUserReference = new AtomicReference<>();
    private volatile UserGroupInformation ugi;

    // Holder of cached Configuration information so validation does not reload the same config over and over
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();

    static final PropertyDescriptor METASTORE_URI = new PropertyDescriptor.Builder()
            .name("hive-metastore-uri")
            .displayName("Hive Metastore URI")
            .description("The URI location(s) for the Hive metastore. This is a comma-separated list of Hive metastore URIs; note that this is not the location of the Hive Server. "
                    + "The default port for the Hive metastore is 9043. If this field is not set, then the 'hive.metastore.uris' property from any provided configuration resources "
                    + "will be used, and if none are provided, then the default value from a default hive-site.xml will be used (usually thrift://localhost:9083).")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URI_LIST_VALIDATOR)
            .build();

    static final PropertyDescriptor HIVE_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hive-config-resources")
            .displayName("Hive Configuration Resources")
            .description("A file or comma separated list of files which contains the Hive configuration (hive-site.xml, e.g.). Without this, Hadoop "
                    + "will search the classpath for a 'hive-site.xml' file or will revert to a default configuration. Note that to enable authentication "
                    + "with Kerberos e.g., the appropriate properties must be set in the configuration files. Please see the Hive documentation for more details.")
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor EVENT_TYPE = new PropertyDescriptor.Builder()
            .name("event-type")
            .displayName("Event Type")
            .description("The type of the event. The acceptable values are 'put' in case of data insert or 'delete' in case of data removal.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${event.type}")
            .build();

    static final PropertyDescriptor PATH = new PropertyDescriptor.Builder()
            .name("path")
            .displayName("Path")
            .description("The path of the file or folder located in the file system.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("${path}")
            .build();

    static final PropertyDescriptor CATALOG_NAME = new PropertyDescriptor.Builder()
            .name("catalog-name")
            .displayName("Catalog Name")
            .description("The name of the catalog.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("database-name")
            .displayName("Database Name")
            .description("The name of the database.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("table-name")
            .displayName("Table Name")
            .description("The name of the table.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Specifies the Kerberos User Controller Service that should be used for authenticating with Kerberos.")
            .identifiesControllerService(KerberosUserService.class)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the data ingestion was successful.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the data ingestion failed and retrying the operation will also fail, such as an invalid data or schema.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            METASTORE_URI,
            HIVE_CONFIGURATION_RESOURCES,
            EVENT_TYPE,
            PATH,
            CATALOG_NAME,
            DATABASE_NAME,
            TABLE_NAME,
            KERBEROS_USER_SERVICE
    ));

    public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS,
            REL_FAILURE
    )));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>();
        final boolean confFileProvided = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).isSet();

        if (confFileProvided) {
            final boolean kerberosUserServiceIsSet = validationContext.getProperty(KERBEROS_USER_SERVICE).isSet();
            final String configFiles = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
            final Configuration config = hiveConfigurator.getConfigurationForValidation(validationResourceHolder, configFiles, getLogger());

            final boolean securityEnabled = SecurityUtil.isSecurityEnabled(config);
            if (securityEnabled && !kerberosUserServiceIsSet) {
                problems.add(new ValidationResult.Builder()
                        .subject(KERBEROS_USER_SERVICE.getDisplayName())
                        .valid(false)
                        .explanation("Security authentication is set to 'kerberos' in the configuration files but no KerberosUserService is configured.")
                        .build());
            }
        }

        return problems;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        final String configFiles = context.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
        hiveConfig = hiveConfigurator.getConfigurationFromFiles(configFiles);

        if (context.getProperty(METASTORE_URI).isSet()) {
            hiveConfig.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), context.getProperty(METASTORE_URI).evaluateAttributeExpressions().getValue());
        }

        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);

        if (kerberosUserService != null) {
            kerberosUserReference.set(kerberosUserService.createKerberosUser());
            try {
                ugi = hiveConfigurator.authenticate(hiveConfig, kerberosUserReference.get());
            } catch (AuthenticationFailedException e) {
                getLogger().error(e.getMessage(), e);
                throw new ProcessException(e);
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final KerberosUser kerberosUser = kerberosUserReference.get();
        if (kerberosUser == null) {
            doOnTrigger(context, session, flowFile);
        } else {
            try {
                getUgi().doAs((PrivilegedExceptionAction<Void>) () -> {
                    doOnTrigger(context, session, flowFile);
                    return null;
                });

            } catch (Exception e) {
                getLogger().error("Privileged action failed with kerberos user " + kerberosUser, e);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    public void doOnTrigger(ProcessContext context, ProcessSession session, FlowFile flowFile) throws ProcessException {
        String catalogName;
        if (context.getProperty(CATALOG_NAME).isSet()) {
            catalogName = context.getProperty(CATALOG_NAME).evaluateAttributeExpressions().getValue();
        } else {
            catalogName = MetaStoreUtils.getDefaultCatalog(hiveConfig);
        }

        final String eventType = context.getProperty(EVENT_TYPE).evaluateAttributeExpressions(flowFile).getValue();
        final String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String path = context.getProperty(PATH).evaluateAttributeExpressions(flowFile).getValue();

        try (final HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(hiveConfig)) {
            final Table table = metaStoreClient.getTable(catalogName, databaseName, tableName);
            final boolean isPartitioned = !table.getPartitionKeys().isEmpty();

            switch (eventType.toLowerCase().trim()) {
                case "put":
                    if (isPartitioned) {
                        EventMessage.EventType eventMessageType = handlePartitionedInsert(metaStoreClient, catalogName, databaseName, tableName, path);
                        flowFile = session.putAttribute(flowFile, METASTORE_NOTIFICATION_EVENT, eventMessageType.toString());
                    } else {
                        handleFileInsert(metaStoreClient, catalogName, databaseName, tableName, path, null);
                        flowFile = session.putAttribute(flowFile, METASTORE_NOTIFICATION_EVENT, EventMessage.EventType.INSERT.toString());
                    }
                    break;
                case "delete":
                    if (isPartitioned) {
                        handleDropPartition(metaStoreClient, catalogName, databaseName, tableName, path);
                        flowFile = session.putAttribute(flowFile, METASTORE_NOTIFICATION_EVENT, EventMessage.EventType.DROP_PARTITION.toString());
                    } else {
                        getLogger().warn("The target table '{}' is not partitioned. No metastore action was executed.", tableName);
                    }
                    break;
                default:
                    getLogger().error("Unknown event type '{}'", eventType);
                    session.transfer(flowFile, REL_FAILURE);
                    return;
            }
        } catch (Exception e) {
            getLogger().error("Error occurred while metastore event processing", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    /**
     * Handles an insert event for partitioned table. If the partition already exists then an insert event will be executed in the metastore else a new partition will be added to the table.
     *
     * @param catalogName  name of the catalog
     * @param databaseName name of the database
     * @param tableName    name of the table
     * @param path         path for the file or directory
     * @return type of the executed event
     * @throws TException thrown if the metastore action fails
     */
    private EventMessage.EventType handlePartitionedInsert(HiveMetaStoreClient metaStoreClient, String catalogName,
                                                           String databaseName, String tableName, String path) throws TException {
        final List<String> partitionValues = getPartitionValuesFromPath(path);

        try {
            // Check if the partition already exists for the file to be inserted
            metaStoreClient.getPartition(databaseName, tableName, partitionValues);
            getLogger().debug("Creating file insert for partition with values {}", partitionValues);
            handleFileInsert(metaStoreClient, catalogName, databaseName, tableName, path, partitionValues);
            return EventMessage.EventType.INSERT;
        } catch (Exception e) {
            if (e instanceof NoSuchObjectException) {
                getLogger().debug("Partition with values {} does not exists. Trying to append new partition", partitionValues, e);
                try {
                    metaStoreClient.appendPartition(catalogName, databaseName, tableName, partitionValues);
                    return EventMessage.EventType.ADD_PARTITION;
                } catch (TException ex) {
                    throw new TException("Failed to append partition with values " + partitionValues, ex);
                }
            }
            throw new TException("Error occurred during partitioned file insertion with values " + partitionValues, e);
        }
    }

    /**
     * Parse the partition values from the file or directory path.
     * e.g.: hdfs://localhost:9000/user/hive/warehouse/table/a=5/b=10/file -> partition values are [5,10]
     *
     * @param path path for the file or directory
     * @return partition values
     */
    private List<String> getPartitionValuesFromPath(String path) {
        final String[] pathParts = path.split("/");
        List<String> partitionValues = new ArrayList<>();
        for (String pathPart : pathParts) {
            if (pathPart.contains("=")) {
                final String[] partitionParts = pathPart.split("=");
                partitionValues.add(partitionParts[1]);
            }
        }
        getLogger().debug("The following partition values were processed from path '{}': {}", path, partitionValues);
        return partitionValues;
    }

    /**
     * Constructs a file insert event and send it into the metastore.
     *
     * @param catalogName     name of the catalog
     * @param databaseName    name of the database
     * @param tableName       name of the table
     * @param path            path for the file or directory
     * @param partitionValues partition values
     * @throws IOException thrown if there are any error with the checksum generation
     * @throws TException  thrown if the metastore action fails
     */
    private void handleFileInsert(HiveMetaStoreClient metaStoreClient, String catalogName, String databaseName,
                                  String tableName, String path, List<String> partitionValues) throws IOException, TException {
        final InsertEventRequestData insertEventRequestData = new InsertEventRequestData();
        insertEventRequestData.setReplace(false);
        insertEventRequestData.addToFilesAdded(path);
        insertEventRequestData.addToFilesAddedChecksum(checksumFor(path));

        final FireEventRequestData fireEventRequestData = new FireEventRequestData();
        fireEventRequestData.setInsertData(insertEventRequestData);

        final FireEventRequest fireEventRequest = new FireEventRequest(true, fireEventRequestData);
        fireEventRequest.setCatName(catalogName);
        fireEventRequest.setDbName(databaseName);
        fireEventRequest.setTableName(tableName);
        if (partitionValues != null) {
            fireEventRequest.setPartitionVals(partitionValues);
        }

        metaStoreClient.fireListenerEvent(fireEventRequest);
    }

    /**
     * Triggers a drop partition event in the metastore with the partition values parsed from the provided path.
     *
     * @param catalogName  name of the catalog
     * @param databaseName name of the database
     * @param tableName    name of the table
     * @param path         path for the file or directory
     * @throws TException thrown if the metastore action fails
     */
    private void handleDropPartition(HiveMetaStoreClient metaStoreClient, String catalogName, String databaseName,
                                     String tableName, String path) throws TException {
        final List<String> partitionValues = getPartitionValuesFromPath(path);
        try {
            metaStoreClient.dropPartition(catalogName, databaseName, tableName, partitionValues, true);
        } catch (TException e) {
            if (e instanceof NoSuchObjectException) {
                getLogger().error("Failed to drop partition. Partition with values {} does not exists.", partitionValues, e);
            }
            throw new TException(e);
        }
    }

    /**
     * Generates checksum for file on the given path if the FileSystem supports it.
     *
     * @param filePath path for the file
     * @return checksum for the file.
     * @throws IOException thrown if there are any issue getting the FileSystem
     */
    private String checksumFor(String filePath) throws IOException {
        final Path path = new Path(filePath);
        final FileSystem fileSystem = path.getFileSystem(hiveConfig);
        final FileChecksum checksum = fileSystem.getFileChecksum(path);
        if (checksum != null) {
            return StringUtils.byteToHexString(checksum.getBytes(), 0, checksum.getLength());
        }
        return "";
    }

    /**
     * Checks if the user TGT expired and performs a re-login if needed.
     *
     * @return ugi
     */
    private UserGroupInformation getUgi() {
        KerberosUser kerberosUser = kerberosUserReference.get();
        try {
            kerberosUser.checkTGTAndRelogin();
        } catch (KerberosLoginException e) {
            throw new ProcessException("Unable to re-login with kerberos credentials for " + kerberosUser.getPrincipal(), e);
        }
        return ugi;
    }
}
