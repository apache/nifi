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
package org.apache.nifi.processors.kite;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.SchemaNotFoundException;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.spi.DefaultConfiguration;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.processor.ProcessorInitializationContext;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

abstract class AbstractKiteProcessor extends AbstractProcessor {

    protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
        return new KerberosProperties(kerberosConfigFile);
    }

    protected KerberosProperties kerberosProperties;
    private volatile File kerberosConfigFile = null;
    private static long kerberosReloginThreshold;
    private static long lastKerberosReloginTime;
    private String kerberosPrincipal;
    private String kerberosKeytab;
    private Configuration configuration;
    private static UserGroupInformation ugi;
    private static FileSystem fs;

    private final static AtomicReference<HdfsResources> hdfsResources = new AtomicReference<>();

    private static final Splitter COMMA = Splitter.on(',').trimResults();
    protected static final Validator FILES_EXIST = new Validator() {
        @Override
        public ValidationResult validate(String subject, String configFiles,
                ValidationContext context) {
            if (configFiles != null && !configFiles.isEmpty()) {
                for (String file : COMMA.split(configFiles)) {
                    ValidationResult result = StandardValidators.FILE_EXISTS_VALIDATOR
                            .validate(subject, file, context);
                    if (!result.isValid()) {
                        return result;
                    }
                }
            }
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(configFiles)
                    .explanation("Files exist")
                    .valid(true)
                    .build();
        }
    };

    protected static final Validator RECOGNIZED_URI = new Validator() {
        @Override
        public ValidationResult validate(String subject, String uri, ValidationContext context) {
            String message = "not set";
            boolean isValid = true;

            if (uri.trim().isEmpty()) {
                isValid = false;
            } else {
                final boolean elPresent = context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(uri);
                if (!elPresent) {
                    try {
                        new URIBuilder(URI.create(uri)).build();
                    } catch (RuntimeException e) {
                        message = e.getMessage();
                        isValid = false;
                    }
                }
            }

            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(uri)
                    .explanation("Dataset URI is invalid: " + message)
                    .valid(isValid)
                    .build();
        }
    };

    protected static final Validator SCHEMA_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String uri, ValidationContext context) {
            Configuration conf = getConfiguration(context.getProperty(CONF_XML_FILES).getValue());
            String error = null;

            final boolean elPresent = context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(uri);
            if (!elPresent) {
                try {
                    getSchema(uri, conf);
                } catch (SchemaNotFoundException e) {
                    error = e.getMessage();
                }
            }
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(uri)
                    .explanation(error)
                    .valid(error == null)
                    .build();
        }
    };

    protected static final PropertyDescriptor CONF_XML_FILES = new PropertyDescriptor.Builder()
            .name("Hadoop configuration files")
            .description("A comma-separated list of Hadoop configuration files")
            .addValidator(FILES_EXIST)
            .build();

    public static final PropertyDescriptor KERBEROS_RELOGIN_PERIOD = new PropertyDescriptor.Builder()
            .name("Kerberos Relogin Period")
            .required(false)
            .description("Period of time which should pass before attempting a kerberos relogin")
            .defaultValue("4 hours")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * Resolves a {@link Schema} for the given string, either a URI or a JSON literal.
     */
    protected static Schema getSchema(String uriOrLiteral, Configuration conf) {
        URI uri;
        try {
            uri = new URI(uriOrLiteral);
        } catch (URISyntaxException e) {
            // try to parse the schema as a literal
            return parseSchema(uriOrLiteral);
        }

        try {
            if ("dataset".equals(uri.getScheme()) || "view".equals(uri.getScheme())) {
                return Datasets.load(uri).getDataset().getDescriptor().getSchema();
            } else if ("resource".equals(uri.getScheme())) {
                try (InputStream in = Resources.getResource(uri.getSchemeSpecificPart())
                        .openStream()) {
                    return parseSchema(uri, in);
                }
            } else {
                try {
                    InputStream in = new FileInputStream(uri.toString());
                    return parseSchema(uri, in);
                } catch (FileNotFoundException e){
                    // Try to open the file in HDFS
                    Path schemaPath = new Path(uri);
                    // Get UGI based on relogin period
                    UserGroupInformation triggeredUgi = getUserGroupInformation();

                    return getSchema(uri, triggeredUgi);
                }
            }

        } catch (DatasetNotFoundException e) {
            throw new SchemaNotFoundException(
                    "Cannot read schema of missing dataset: " + uri, e);
        } catch (IOException e) {
            throw new SchemaNotFoundException(
                    "Failed while reading " + uri + ": " + e.getMessage(), e);
        }
    }

    private static Schema parseSchema(String literal) {
        try {
            return new Schema.Parser().parse(literal);
        } catch (RuntimeException e) {
            throw new SchemaNotFoundException(
                    "Failed to parse schema: " + literal, e);
        }
    }

    private static Schema parseSchema(URI uri, InputStream in) throws IOException {
        try {
            return new Schema.Parser().parse(in);
        } catch (RuntimeException e) {
            throw new SchemaNotFoundException("Failed to parse schema at " + uri, e);
        }
    }

    @OnScheduled
    protected void setDefaultConfiguration(ProcessContext context)
            throws IOException {
        DefaultConfiguration.set(getConfiguration(
                context.getProperty(CONF_XML_FILES).getValue()));
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        hdfsResources.set(new HdfsResources(null, null, null));

        kerberosConfigFile = context.getKerberosConfigurationFile();
        kerberosProperties = getKerberosProperties(kerberosConfigFile);

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(CONF_XML_FILES);
        props.add(KERBEROS_RELOGIN_PERIOD);
        props.add(kerberosProperties.getKerberosPrincipal());
        props.add(kerberosProperties.getKerberosKeytab());
        ABSTRACT_KITE_PROPS = Collections.unmodifiableList(props);
    }

    protected List<PropertyDescriptor> ABSTRACT_KITE_PROPS;

    protected List<PropertyDescriptor> getProperties() {
        return ABSTRACT_KITE_PROPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ABSTRACT_KITE_PROPS;
    }

    @OnScheduled
    public void abstractOnScheduled(ProcessContext context) throws Exception {
        if (context.getProperty(KERBEROS_RELOGIN_PERIOD).getValue() != null) {
            kerberosReloginThreshold = context.getProperty(KERBEROS_RELOGIN_PERIOD).asTimePeriod(TimeUnit.SECONDS);
        }
        configuration = getConfiguration(context.getProperty(CONF_XML_FILES).getValue());
        if (SecurityUtil.isSecurityEnabled(configuration)) {
            kerberosPrincipal = context.getProperty(kerberosProperties.getKerberosPrincipal()).getValue();
            kerberosKeytab = context.getProperty(kerberosProperties.getKerberosKeytab()).getValue();
            ugi = SecurityUtil.loginKerberos(configuration, kerberosPrincipal, kerberosKeytab);
            // Set last Kerberos relogin
            lastKerberosReloginTime = System.currentTimeMillis() / 1000;
        } else {
            configuration.set("ipc.client.fallback-to-simple-auth-allowed", "true");
            configuration.set("hadoop.security.authentication", "simple");
            ugi = SecurityUtil.loginSimple(configuration);
        }
        fs = getFileSystemAsUser(configuration, ugi);
        hdfsResources.set(new HdfsResources(configuration, fs, ugi));
    }

    protected static Schema getSchema(URI uri, UserGroupInformation ugi) throws IOException {
        FileSystem triggeredFs = hdfsResources.get().getFileSystem();
        Path schemaPath = new Path(uri);
        try {
            return ugi.doAs(new PrivilegedExceptionAction<Schema>() {
                @Override
                public Schema run() throws IOException {
                    try (InputStream in = triggeredFs.open(schemaPath)){
                        return parseSchema(uri, in);
                    } catch (IOException e) {
                        throw new IOException("Unable to open schema file: " + e.getMessage());
                    }
                }
            });
        } catch (InterruptedException e) {
            throw new IOException("Unable to open schema file: " + e.getMessage());
        }
    }


    protected static FileSystem getFileSystemAsUser(final Configuration config, UserGroupInformation ugi) throws IOException {
        try {
            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                @Override
                public FileSystem run() throws Exception {
                    return FileSystem.get(config);
                }
            });
        } catch (InterruptedException e) {
            throw new IOException("Unable to create file system: " + e.getMessage());
        }
    }

    protected static boolean isTicketOld() {
        return (System.currentTimeMillis() / 1000 - lastKerberosReloginTime) > kerberosReloginThreshold;
    }

    protected static void tryKerberosRelogin(UserGroupInformation ugi) {
        try {
            // Kerberos ticket aged exceeds threshold; attempting to renew ticket for user
            ugi.checkTGTAndReloginFromKeytab();
            lastKerberosReloginTime = System.currentTimeMillis() / 1000;
            // Kerberos relogin successful or ticket still valid
        } catch (IOException e) {
            // Kerberos relogin failed
            // Most likely case of this happening is ticket expired and error getting a new one, meaning HDFS operations would fail
            throw new ProcessException("Unable to renew kerberos ticket", e);
        }
    }

    static protected class HdfsResources {
        private final Configuration configuration;
        private final FileSystem fileSystem;
        private final UserGroupInformation userGroupInformation;

        public HdfsResources(Configuration configuration, FileSystem fileSystem, UserGroupInformation userGroupInformation) {
            this.configuration = configuration;
            this.fileSystem = fileSystem;
            this.userGroupInformation = ugi;
        }

        public Configuration getConfiguration() {
            return configuration;
        }

        public FileSystem getFileSystem() {
            return fileSystem;
        }

        public UserGroupInformation getUserGroupInformation() {
            return userGroupInformation;
        }
    }

    protected FileSystem getFileSystem() {
        // trigger relogin if necessary
        getUserGroupInformation();
        return hdfsResources.get().getFileSystem();
    }

    protected static UserGroupInformation getUserGroupInformation() {
        UserGroupInformation userGroupInformation = hdfsResources.get().getUserGroupInformation();
        if (userGroupInformation != null || isTicketOld()) {
            tryKerberosRelogin(userGroupInformation);
        }

        return userGroupInformation;
    }

    protected static Configuration getConfiguration(String configFiles) {
        Configuration conf = DefaultConfiguration.get();

        if (configFiles == null || configFiles.isEmpty()) {
            return conf;
        }

        for (String file : COMMA.split(configFiles)) {
            // process each resource only once
            if (conf.getResource(file) == null) {
                // use Path instead of String to get the file from the FS
                conf.addResource(new Path(file));
            }
        }

        return conf;
    }
}
