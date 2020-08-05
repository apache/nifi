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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.net.URI;

import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2CreateOptions;
import com.hierynomus.smbj.share.File;
import java.io.OutputStream;
import java.util.EnumSet;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"samba, smb, cifs, files, put"})
@CapabilityDescription("Writes the contents of a FlowFile to a samba network location. " +
    "Use this processor instead of a cifs mounts if share access control is important." +
    "Configure the Hostname, Share and Directory accordingly: \\\\[Hostname]\\[Share]\\[path\\to\\Directory]")
@SeeAlso({GetSmbFile.class})
@ReadsAttributes({@ReadsAttribute(attribute="filename", description="The filename to use when writing the FlowFile to the network folder.")})
public class PutSmbFile extends AbstractProcessor {
    public static final String SHARE_ACCESS_NONE = "none";
    public static final String SHARE_ACCESS_READ = "read";
    public static final String SHARE_ACCESS_READDELETE = "read, delete";
    public static final String SHARE_ACCESS_READWRITEDELETE = "read, write, delete";

    public static final String REPLACE_RESOLUTION = "replace";
    public static final String IGNORE_RESOLUTION = "ignore";
    public static final String FAIL_RESOLUTION = "fail";

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The network host to which files should be written.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SHARE = new PropertyDescriptor.Builder()
            .name("Share")
            .description("The network share to which files should be written. This is the \"first folder\"" +
            "after the hostname: \\\\hostname\\[share]\\dir1\\dir2")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The network folder to which files should be written. This is the remaining relative " +
            "path after the share: \\\\hostname\\share\\[dir1\\dir2]. You may use expression language.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor DOMAIN = new PropertyDescriptor.Builder()
            .name("Domain")
            .description("The domain used for authentication. Optional, in most cases username and password is sufficient.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username used for authentication. If no username is set then anonymous authentication is attempted.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password used for authentication. Required if Username is set.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor CREATE_DIRS = new PropertyDescriptor.Builder()
            .name("Create Missing Directories")
            .description("If true, then missing destination directories will be created. If false, flowfiles are penalized and sent to failure.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor SHARE_ACCESS = new PropertyDescriptor.Builder()
            .name("Share Access Strategy")
            .description("Indicates which shared access are granted on the file during the write. " +
                "None is the most restrictive, but the safest setting to prevent corruption.")
            .required(true)
            .defaultValue(SHARE_ACCESS_NONE)
            .allowableValues(SHARE_ACCESS_NONE, SHARE_ACCESS_READ, SHARE_ACCESS_READDELETE, SHARE_ACCESS_READWRITEDELETE)
            .build();
    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(REPLACE_RESOLUTION)
            .allowableValues(REPLACE_RESOLUTION, IGNORE_RESOLUTION, FAIL_RESOLUTION)
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of files to put in each iteration")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to the output network path are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to the output network path for some reason are transferred to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private SMBClient smbClient = null; // this gets synchronized when the `connect` method is called
    private Set<SMB2ShareAccess> sharedAccess;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(HOSTNAME);
        descriptors.add(SHARE);
        descriptors.add(DIRECTORY);
        descriptors.add(DOMAIN);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(CREATE_DIRS);
        descriptors.add(SHARE_ACCESS);
        descriptors.add(CONFLICT_RESOLUTION);
        descriptors.add(BATCH_SIZE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        if (this.smbClient == null) {
            initSmbClient();
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

        switch (context.getProperty(SHARE_ACCESS).getValue()) {
            case SHARE_ACCESS_NONE:
                sharedAccess = Collections.<SMB2ShareAccess>emptySet();
                break;
            case SHARE_ACCESS_READ:
                sharedAccess = EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ);
                break;
            case SHARE_ACCESS_READDELETE:
                sharedAccess = EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ, SMB2ShareAccess.FILE_SHARE_DELETE);
                break;
            case SHARE_ACCESS_READWRITEDELETE:
                sharedAccess = EnumSet.of(SMB2ShareAccess.FILE_SHARE_READ, SMB2ShareAccess.FILE_SHARE_WRITE, SMB2ShareAccess.FILE_SHARE_DELETE);
                break;
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = Collections.emptySet();
        if (validationContext.getProperty(USERNAME).isSet() && !validationContext.getProperty(PASSWORD).isSet()) {
            set.add(new ValidationResult.Builder().explanation("Password must be set if username is supplied.").build());
        }
        return set;
    }

    private void initSmbClient() {
        initSmbClient(new SMBClient());
    }

    void initSmbClient(SMBClient smbClient) {
        this.smbClient = smbClient;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        List<FlowFile> flowFiles = session.get(batchSize);
        if ( flowFiles.isEmpty() ) {
            return;
        }
        final ComponentLog logger = getLogger();
        logger.debug("Processing next {} flowfiles", new Object[]{flowFiles.size()});

        final String hostname = context.getProperty(HOSTNAME).getValue();
        final String shareName = context.getProperty(SHARE).getValue();

        final String domain = context.getProperty(DOMAIN).getValue();
        final String username = context.getProperty(USERNAME).getValue();
        String password = context.getProperty(PASSWORD).getValue();

        AuthenticationContext ac = null;
        if (username != null && password != null) {
            ac = new AuthenticationContext(
                username,
                password.toCharArray(),
                domain);
        } else {
            ac = AuthenticationContext.anonymous();
        }

        try (Connection connection = smbClient.connect(hostname);
            Session smbSession = connection.authenticate(ac);
            DiskShare share = (DiskShare) smbSession.connectShare(shareName)) {

            for (FlowFile flowFile : flowFiles) {
                String directory = context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue();
                final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
                final long sendStart = System.nanoTime();
                String fullPath;

                if (directory == null) {
                    directory = "";
                    fullPath = filename;
                } else {
                    fullPath = directory + "\\" + filename;

                    // missing directory handling
                    if (context.getProperty(CREATE_DIRS).asBoolean() && !share.folderExists(directory)) {
                        logger.debug("Creating folder {}", new Object[]{directory});
                        share.mkdir(directory);
                    }
                }

                final URI uri = new URI("smb", hostname, "/" + fullPath.replace('\\', '/'), null);

                // replace strategy handling
                SMB2CreateDisposition createDisposition = SMB2CreateDisposition.FILE_OVERWRITE_IF;
                final String conflictResolution = context.getProperty(CONFLICT_RESOLUTION).getValue();
                if (!conflictResolution.equals(REPLACE_RESOLUTION) && share.fileExists(fullPath)) {
                    if (conflictResolution.equals(IGNORE_RESOLUTION)) {
                        session.transfer(flowFile, REL_SUCCESS);
                        logger.info("Transferring {} to success because file with same name already exists", new Object[]{flowFile});
                        continue;
                    } else if (conflictResolution.equals(FAIL_RESOLUTION)) {
                        flowFile = session.penalize(flowFile);
                        logger.warn("Penalizing {} and routing to failure as configured because file with the same name already exists", new Object[]{flowFile});
                        session.transfer(flowFile, REL_FAILURE);
                        continue;
                    }
                }


                try (File f = share.openFile(
                        fullPath,
                        EnumSet.of(AccessMask.GENERIC_WRITE),
                        EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL),
                        sharedAccess,
                        createDisposition,
                        EnumSet.of(SMB2CreateOptions.FILE_WRITE_THROUGH));
                    OutputStream os = f.getOutputStream()) {

                    session.exportTo(flowFile, os);

                    final long sendNanos = System.nanoTime() - sendStart;
                    final long sendMillis = TimeUnit.MILLISECONDS.convert(sendNanos, TimeUnit.NANOSECONDS);
                    session.getProvenanceReporter().send(flowFile, uri.toString(), sendMillis);
                    session.transfer(flowFile, REL_SUCCESS);
                } catch (Exception e) {
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                    logger.error("Penalizing {} and routing to 'failure' because of error {}", new Object[]{flowFile, e});
                }
            }
        } catch (Exception e) {
            session.transfer(flowFiles, REL_FAILURE);
            logger.error("Could not establish smb connection because of error {}", new Object[]{e});
        }
    }
}
