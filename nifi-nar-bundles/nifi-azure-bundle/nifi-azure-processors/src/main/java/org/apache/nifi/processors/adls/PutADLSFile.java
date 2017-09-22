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
package org.apache.nifi.processors.adls;

import com.microsoft.azure.datalake.store.ADLFileOutputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.acl.AclEntry;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.adls.ADLSConstants.ERR_ACL_ENTRY;
import static org.apache.nifi.processors.adls.ADLSConstants.ERR_FLOWFILE_CORE_ATTR_FILENAME;
import static org.apache.nifi.processors.adls.ADLSConstants.REL_FAILURE;
import static org.apache.nifi.processors.adls.ADLSConstants.FILE_NAME_ATTRIBUTE;
import static org.apache.nifi.processors.adls.ADLSConstants.CHUNK_SIZE_IN_BYTES;
import static org.apache.nifi.processors.adls.ADLSConstants.ADLS_FILE_PATH_ATTRIBUTE;
import static org.apache.nifi.processors.adls.ADLSConstants.REL_SUCCESS;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"azure", "hadoop", "ADLS", "put", "egress", "copy", "filesystem", "restricted"})
@CapabilityDescription("Write FlowFile data to Azure Data Lake Store (ADLS)")
@SeeAlso({ListADLSFile.class, FetchADLSFile.class})
@ReadsAttributes({
        @ReadsAttribute(attribute = "filename", description = "The name of the file written to ADLS comes from the value of this attribute."),
})
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file written to ADLS is stored in this attribute."),
        @WritesAttribute(attribute = "absolute.adls.path", description = "The absolute path to the file on ADLS is stored in this attribute.")
})
@Restricted("Provides operator the ability to write to any file that NiFi has access to in ADLS.")
public class PutADLSFile extends ADLSAbstractProcessor {

    private static final String PART_FILE_EXTENSION = ".nifipart";
    private static final String REPLACE_RESOLUTION = "replace";
    private static final String FAIL_RESOLUTION = "fail";
    private static final String APPEND_RESOLUTION = "append";

    protected static final AllowableValue REPLACE_RESOLUTION_AV = new AllowableValue(REPLACE_RESOLUTION,
            REPLACE_RESOLUTION, "Replaces the existing file if any.");
    protected static final AllowableValue FAIL_RESOLUTION_AV = new AllowableValue(FAIL_RESOLUTION, FAIL_RESOLUTION,
            "Penalizes the flow file and routes it to failure.");
    protected static final AllowableValue APPEND_RESOLUTION_AV = new AllowableValue(APPEND_RESOLUTION, APPEND_RESOLUTION,
            "Appends to the existing file if any, creates a new file otherwise.");

    // properties
    protected static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("adls-put-conflict-resolution-strategy")
            .displayName("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file " +
                    "with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION_AV.getValue())
            .allowableValues(REPLACE_RESOLUTION_AV, FAIL_RESOLUTION_AV, APPEND_RESOLUTION_AV)
            .build();

    protected static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("adls-put-directory")
            .displayName("Directory")
            .description("The directory where files will be written")
            .required(true)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    protected static final PropertyDescriptor UMASK = new PropertyDescriptor.Builder()
            .name("adls-put-permissions-umask")
            .displayName("Permissions umask")
            .description(
                    "A umask represented as an octal number which determines the permissions of files written to ADLS.")
            .addValidator(ADLSValidators.UMASK_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ACL = new PropertyDescriptor.Builder()
            .name("adls-put-access-control-list")
            .displayName("Access Control List")
            .description(
                    "Comma separated list of ACL entry. An ACL entry consists of a scope (access or default)" +
                            " the type of the ACL (user, group, other or mask), the name of the user or group" +
                            " associated with this ACL (can be blank to specify the default permissions for" +
                            " users and groups, and must be blank for mask entries), and the action permitted" +
                            " by this ACL entry. eg: \"default:user:bob:r-x,default:user:adam:rw\"")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        super.descriptors.add(CONFLICT_RESOLUTION);
        super.descriptors.add(DIRECTORY);
        super.descriptors.add(UMASK);
        super.descriptors.add(ACL);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch dataRateStopWatch = new StopWatch();
        final StopWatch completeProcessStopWatch = new StopWatch(true);

        ADLStoreClient adlsClient = getAdlStoreClient();
        //safe check
        if(adlsClient == null) {
            createADLSClient(context);
            adlsClient = getAdlStoreClient();
        }
        final ADLStoreClient adlsClientFinal = adlsClient;
        final String coreAttributeFileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        final String conflictResolution = context.getProperty(CONFLICT_RESOLUTION).getValue();
        Path directoryPath = Paths.get(context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue());
        final String octalPermission = context.getProperty(UMASK).getValue();
        final String acl = context.getProperty(ACL).getValue();

        try {
            final List<AclEntry> aclList;
            try {
                aclList = parseACL(acl);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(ERR_ACL_ENTRY, e);
            }
            if (StringUtils.isEmpty(coreAttributeFileName))
                throw new IllegalArgumentException(ERR_FLOWFILE_CORE_ATTR_FILENAME);
            final String filePath = directoryPath.resolve(coreAttributeFileName).toString();
            // feel free to overwrite this assuming it's corrupt temp file from last time
            final String tempFilePath = filePath + PART_FILE_EXTENSION;
            try {
                final ADLFileOutputStream fileOutputStream =
                        createADLSFile(adlsClientFinal, tempFilePath, octalPermission);

                dataRateStopWatch.start();
                uploadFlowFile(session, flowFile, fileOutputStream);
                dataRateStopWatch.stop();

                if(!resolveADLSFileBasedOnConflictPolicy(adlsClientFinal, filePath, tempFilePath, conflictResolution))
                    throw new IOException("Failed to resolve temp file into expected file");
            } finally {
                try {
                    adlsClientFinal.delete(tempFilePath);
                } catch (IOException e) {
                    //ignore
                }
            }

            if (!aclList.isEmpty())
                adlsClientFinal.setAcl(filePath, aclList);

            addFlowFileAttributes(session, flowFile, coreAttributeFileName, filePath);
            session.transfer(flowFile, REL_SUCCESS);
            completeProcessStopWatch.stop();
            session.getProvenanceReporter().send(
                    flowFile,
                    getTransitURI(filePath),
                    completeProcessStopWatch.getDuration(TimeUnit.MILLISECONDS));
            getLogger().debug("Successfully uploaded the file in {} at {} rate",
                    new Object[]{
                            dataRateStopWatch.getDuration(),
                            dataRateStopWatch.calculateDataRate(flowFile.getSize())
                    });
        } catch (ProcessException | IOException | IllegalArgumentException e) {
            if(e instanceof ProcessException && !(e.getCause() instanceof IOException))
                throw (ProcessException)e;
            if(e instanceof IllegalArgumentException
                    && !(e.getMessage().equals(ERR_ACL_ENTRY))
                    && !(e.getMessage().equals(ERR_FLOWFILE_CORE_ATTR_FILENAME)))
                throw (IllegalArgumentException)e;
            getLogger().error("Unable to upload the file to ADLS", e);
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }
    }

    private ADLFileOutputStream createADLSFile(
            ADLStoreClient adlsClientFinal,
            String filePath,
            String octalPermission)
            throws IOException {
        return adlsClientFinal.createFile(
                filePath,
                IfExists.OVERWRITE,
                octalPermission,
                true);
    }

    private boolean resolveADLSFileBasedOnConflictPolicy(
            ADLStoreClient adlsClientFinal,
            String filePath,
            String tempFilePath,
            String conflictResolution) throws IOException {
        if (APPEND_RESOLUTION.equals(conflictResolution)) {
            return adlsClientFinal.concatenateFiles(filePath,
                    new ArrayList<>(Arrays.asList(tempFilePath)));
        } else
            return adlsClientFinal.rename(tempFilePath, filePath,
                    REPLACE_RESOLUTION.equals(conflictResolution));
    }

    private void addFlowFileAttributes(
            ProcessSession session,
            FlowFile flowFile,
            String coreAttributeFileName,
            String filePath) {
        session.putAttribute(flowFile, FILE_NAME_ATTRIBUTE, coreAttributeFileName);
        session.putAttribute(flowFile, ADLS_FILE_PATH_ATTRIBUTE, filePath);
    }

    private void uploadFlowFile(
            ProcessSession session,
            FlowFile flowFile,
            ADLFileOutputStream fileOutputStream) {
        session.read(flowFile, inputStream ->
        {
            try {
                byte[] buffer = new byte[CHUNK_SIZE_IN_BYTES];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    fileOutputStream.write(buffer, 0, bytesRead);
                }
            } finally {
                if (fileOutputStream != null) {
                    //this flushes as well
                    fileOutputStream.close();
                }
            }
        });
    }

    private List<AclEntry> parseACL(String acl) {
        if(StringUtils.isEmpty(acl))
            return Collections.emptyList();
        return Arrays.stream(
                acl.split(","))
                .map(aclValue -> AclEntry.parseAclEntry(aclValue))
                .collect(Collectors.toList());
    }
}