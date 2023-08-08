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
package org.apache.nifi.processors.smb;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.session.Session;
import com.rapid7.client.dcerpc.RPCException;
import com.rapid7.client.dcerpc.dto.SID;
import com.rapid7.client.dcerpc.mserref.SystemErrorCode;
import com.rapid7.client.dcerpc.mslsad.LocalSecurityAuthorityService;
import com.rapid7.client.dcerpc.mslsad.dto.PolicyHandle;
import com.rapid7.client.dcerpc.transport.RPCTransport;
import com.rapid7.client.dcerpc.transport.SMBTransportFactories;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.smb.common.SmbUtils.buildSmbClient;

@Tags({"windows, smb, security, account, permissions"})
@CapabilityDescription("Retrieves permissions for each record in the flowfile. " +
        "Each record must have a field that can be interpreted as a SID. " +
        "The SID is used to retrieve the permissions from the SMB server using LsarEnumerateAccountRights RPC." +
        " The account permissions are added to the record as a new field.")
public class EnumerateAccountRights extends AbstractProcessor {
    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The hostname of the SMB server.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DOMAIN = new PropertyDescriptor.Builder()
            .name("Domain")
            .description("The domain used for authentication. Optional, in most cases username and password is sufficient.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username used for authentication. If no username is set then anonymous authentication is attempted.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password used for authentication. Required if Username is set.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor ACCESS_LEVEL = new PropertyDescriptor.Builder()
            .name("Access level")
            .description("integer representation of policy object access level")
            .required(true)
            .defaultValue("33554432")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AD_SERVER_NAME = new PropertyDescriptor.Builder()
            .name("AD Server Name")
            .description("Name of Active Directory Server")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor SID_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("SID field name")
            .description("Name of record field that contains Active Directory SID. Can be a coma separated list of fields.")
            .required(true)
            .defaultValue("objectSid")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MEMBER_OF_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("MemberOf field name")
            .description("Name of record field that contains groups that the user is a member of")
            .required(false)
            .defaultValue("memberof")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PERMISSIONS_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("Permissions field name")
            .description("Name of record field where permissions will be written")
            .required(true)
            .defaultValue("permissions")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SEPARATOR = new PropertyDescriptor.Builder()
            .name("Array separator")
            .description("This property indicates the separator to use between array elements")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .defaultValue(";")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are successfully transformed will be routed to this relationship")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
                    + "the unchanged FlowFile will be routed to this relationship")
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        properties.add(HOSTNAME);
        properties.add(DOMAIN);
        properties.add(USERNAME);
        properties.add(PASSWORD);
        properties.add(ACCESS_LEVEL);
        properties.add(AD_SERVER_NAME);
        properties.add(SID_FIELD_NAME);
        properties.add(MEMBER_OF_FIELD_NAME);
        properties.add(PERMISSIONS_FIELD_NAME);
        properties.add(SEPARATOR);
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        SMBClient smbClient = initSmbClient(context);
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final String sidFieldName = context.getProperty(SID_FIELD_NAME)
                .evaluateAttributeExpressions(flowFile).getValue();
        final String memberOfFieldName = context.getProperty(MEMBER_OF_FIELD_NAME)
                .evaluateAttributeExpressions(flowFile).getValue();
        final String permissionsFieldName = context.getProperty(PERMISSIONS_FIELD_NAME)
                .evaluateAttributeExpressions(flowFile).getValue();
        final String separator = context.getProperty(SEPARATOR).evaluateAttributeExpressions(flowFile).getValue();
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
        final String domainOrNull = context.getProperty(DOMAIN).isSet() ? context.getProperty(DOMAIN)
                .evaluateAttributeExpressions(flowFile).getValue() : null;
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions(flowFile).getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions(flowFile).getValue();
        final String accessLevel = context.getProperty(ACCESS_LEVEL).evaluateAttributeExpressions(flowFile).getValue();
        final String adServerNameorNull = context.getProperty(AD_SERVER_NAME).isSet()
                ? context.getProperty(AD_SERVER_NAME).evaluateAttributeExpressions(flowFile).getValue() : null;


        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER)
                .asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                .asControllerService(RecordSetWriterFactory.class);

        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCount = new AtomicInteger();

        final FlowFile original = flowFile;
        final Map<String, String> originalAttributes = flowFile.getAttributes();

        AuthenticationContext ac = null;
        if (username != null && password != null) {
            ac = new AuthenticationContext(username, password.toCharArray(), domainOrNull);
        } else {
            ac = AuthenticationContext.anonymous();
        }
        try (Connection connection = smbClient.connect(hostname);
             Session smbSession = connection.authenticate(ac)) {
                final RPCTransport transport = SMBTransportFactories.LSASVC.getTransport(smbSession);
                getLogger().debug("Connected to SMB service");
                final LocalSecurityAuthorityService service = new LocalSecurityAuthorityService(transport);

                PolicyHandle handle = service.openPolicyHandle(adServerNameorNull, Integer.parseInt(accessLevel));

                flowFile = session.write(flowFile, (in, out) -> {

                    try (final RecordReader reader = readerFactory
                            .createRecordReader(originalAttributes, in, original.getSize(), getLogger())) {

                        // Get the first record and process it before we create the Record Writer. We do this so that if the Processor
                        // updates the Record's schema, we can provide an updated schema to the Record Writer. If there are no records,
                        // then we can simply create the Writer with the Reader's schema and begin & end the Record Set.
                        Record firstRecord = reader.nextRecord();
                        if (firstRecord == null) {
                            final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, reader.getSchema());
                            try (final RecordSetWriter writer = writerFactory
                                    .createWriter(getLogger(), writeSchema, out, originalAttributes)) {
                                writer.beginRecordSet();

                                final WriteResult writeResult = writer.finishRecordSet();
                                attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                                attributes.putAll(writeResult.getAttributes());
                            }
                            return;
                        }

                        firstRecord = processADRecord(firstRecord,  sidFieldName, memberOfFieldName, permissionsFieldName, separator, handle, service);

                        final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, firstRecord.getSchema());
                        try (final RecordSetWriter writer = writerFactory
                                .createWriter(getLogger(), writeSchema, out, originalAttributes)) {
                            writer.beginRecordSet();

                            writer.write(firstRecord);

                            Record record;
                            while ((record = reader.nextRecord()) != null) {
                                final Record processed = processADRecord(record, sidFieldName, memberOfFieldName, permissionsFieldName,
                                        separator, handle, service);
                                writer.write(processed);
                            }

                            final WriteResult writeResult = writer.finishRecordSet();
                            attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                            attributes.putAll(writeResult.getAttributes());
                        }
                    } catch (final SchemaNotFoundException e) {
                        throw new ProcessException(e.getLocalizedMessage(), e);
                    } catch (final MalformedRecordException e) {
                        throw new ProcessException("Could not parse incoming data", e);
                    }
                });
        } catch (final Exception e) {
            getLogger().error("Failed to process {}; will route to failure", new Object[] {flowFile, e});
            // Since we are wrapping the exceptions above there should always be a cause
            // but it's possible it might not have a message. This handles that by logging
            // the name of the class thrown.
            Throwable c = e.getCause();
            if (c != null) {
                session.putAttribute(flowFile, "record.error.message", (c.getLocalizedMessage() != null) ? c.getLocalizedMessage() : c.getClass().getCanonicalName() + " Thrown");
            } else {
                session.putAttribute(flowFile, "record.error.message", e.getClass().getCanonicalName() + " Thrown");
            }
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, REL_SUCCESS);

        final int count = recordCount.get();
        session.adjustCounter("Records Processed", count, false);
        getLogger().info("Successfully converted {} records for {}", new Object[] {count, flowFile});
    }

    /**
     * Process a record by adding the account rights to the record. This method uses Rapid7 smbj-rpc to query the
     * local security authority service for the account rights.
     * @param record the record to process
     * @param sidFieldName the name of the field containing the SID
     *                     (can be a comma separated list of fields)
     * @param memberOfFieldName the name of the field containing the member of information
     * @param permissionsFieldName the name of the field to add the account rights to
     * @param separator the separator to use when joining the account rights
     * @param handle the policy handle to use
     * @param service the local security authority service to use
     * @return the processed record
     * @throws IOException if an error occurs
     */
    protected Record processADRecord(Record record, String sidFieldName, String memberOfFieldName, String permissionsFieldName,
                                     String separator, PolicyHandle handle,
                                     LocalSecurityAuthorityService service) throws IOException {
        final String[] sidFieldNames = sidFieldName.split(",");
        ArrayList<String> rightsArray = new ArrayList<>();

        // get permissions for each SID. Normally, this is the account's SID and the primaryGroup's SID
        for (String sidField : sidFieldNames) {
            final String sidString = record.getAsString(sidField);
            if(sidString != null && !sidString.isEmpty()) {
                try {
                    final SID sid = SID.fromString(sidString);
                    rightsArray.addAll(Arrays.asList(service.getAccountRights(handle, sid)));
                    getLogger().debug("Found permissions for {} in {}", sidString, sidField);
                } catch (RPCException rpce) {
                    if (rpce.getErrorCode() == SystemErrorCode.STATUS_OBJECT_NAME_NOT_FOUND) {
                        getLogger().debug("Could not find permissions for {} found in {}", sidString, sidField);
                    } else {
                        getLogger().error("Could not establish smb connection because of error {}", new Object[]{rpce});
                    }
                }
            }
        }

        //Look up SIDs for DNs in memberOf
        final Object[] parentGroups = record.getAsArray(memberOfFieldName);
        if(parentGroups != null && parentGroups.length > 0) {
            List<String> parentNames = new ArrayList<>();
            for (Object parentName : parentGroups) {
                    if(parentName == null || parentName.toString().isEmpty()) {
                        getLogger().debug("Empty string in {}. Skipping", memberOfFieldName);
                        continue;
                    }
                    // extract the first CN=... part of the DN
                    parentNames.add(parentName.toString().split(",")[0].replace("CN=", ""));
            }
            try {
                final SID[] parentSids = service.lookupSIDsForNames(handle, parentNames.toArray(new String[parentNames.size()]));
                if(parentSids == null || parentSids.length == 0) {
                    getLogger().debug("Could not find SIDs for parent groups {}. Skipping", parentNames);
                } else {
                    getLogger().debug("Found {} SIDs for parent groups {}", parentSids.length, parentNames);
                    for (SID parentSid : parentSids) {
                        if (parentSid == null) {
                            continue;
                        }
                        rightsArray.addAll(Arrays.asList(service.getAccountRights(handle, parentSid)));
                        getLogger().debug("Found permissions for parent group {}", parentSid);
                    }
                }
            } catch (RPCException rpce) {
                if (rpce.getErrorCode() == SystemErrorCode.STATUS_OBJECT_NAME_NOT_FOUND) {
                    getLogger().debug("Could not find permissions for parent. Message: {}", rpce.getMessage());
                } else {
                    getLogger().error("Could not establish smb connection because of error {}", new Object[]{rpce});
                }
            }
        } else {
            getLogger().debug("{} is empty ({})", memberOfFieldName, record.getAsString(memberOfFieldName));
        }

        //convert rightsArray to a Set
        getLogger().debug("Found {} permissions", rightsArray.size());
        Set<String> rightsSet = new HashSet<String>(rightsArray);
        // Add permissions field to the record
        record.setValue(permissionsFieldName, String.join(separator, rightsSet));
        getLogger().debug("Added {} permissions to record", rightsSet.size());
        //update record schema
        record.incorporateInactiveFields();
        return record;
    }

    private SMBClient initSmbClient(final ProcessContext context) {
        return buildSmbClient(context);
    }
}
