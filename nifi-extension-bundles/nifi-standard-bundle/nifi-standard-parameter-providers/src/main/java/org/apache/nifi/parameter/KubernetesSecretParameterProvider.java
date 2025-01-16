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
package org.apache.nifi.parameter;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.LimitingInputStream;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Tags({"file"})
@CapabilityDescription("Fetches parameters from files, in the format provided by Kubernetes mounted secrets.  " +
        "Parameter groups are indicated by a set of directories, and files within the directories map to parameter names. " +
        "The content of the file becomes the parameter value.  Since Kubernetes mounted Secrets are base64-encoded, the " +
        "parameter provider defaults to Base64-decoding the value of the parameter from the file.")

@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.READ_FILESYSTEM,
                        explanation = "Provides operator the ability to read from any file that NiFi has access to.")
        }
)
public class KubernetesSecretParameterProvider extends AbstractParameterProvider implements VerifiableParameterProvider  {
    private static final int MAX_SIZE_LIMIT = 8096;
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    private enum ParameterValueEncoding {
        BASE64,
        PLAINTEXT
    }

    private static final AllowableValue BASE64_ENCODING = new AllowableValue("base64", "Base64", "File content is Base64-encoded, " +
            "and will be decoded before providing the value as a Parameter.");
    private static final AllowableValue PLAIN_TEXT = new AllowableValue("plaintext", "Plain text", "File content is not encoded, " +
            "and will be provided directly as a Parameter value.");

    public static final PropertyDescriptor PARAMETER_GROUP_DIRECTORIES = new PropertyDescriptor.Builder()
            .name("parameter-group-directories")
            .displayName("Parameter Group Directories")
            .description("A comma-separated list of directory absolute paths that will map to named parameter groups.  Each directory that contains " +
                    "files will map to a parameter group, named after the innermost directory in the path.  Files inside the directory will map to " +
                    "parameter names, whose values are the content of each respective file.")
            .addValidator(new MultiDirectoryExistsValidator())
            .required(true)
            .build();
    public static final PropertyDescriptor PARAMETER_VALUE_BYTE_LIMIT = new PropertyDescriptor.Builder()
            .name("parameter-value-byte-limit")
            .displayName("Parameter Value Byte Limit")
            .description("The maximum byte size of a parameter value.  Since parameter values are pulled from the contents of files, this is a safeguard that can " +
                    "prevent memory issues if large files are included.")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, MAX_SIZE_LIMIT))
            .defaultValue("256 B")
            .required(true)
            .build();
    public static final PropertyDescriptor PARAMETER_VALUE_ENCODING = new PropertyDescriptor.Builder()
            .name("parameter-value-encoding")
            .displayName("Parameter Value Encoding")
            .description("Indicates how parameter values are encoded inside Parameter files.")
            .allowableValues(BASE64_ENCODING, PLAIN_TEXT)
            .defaultValue(BASE64_ENCODING.getValue())
            .required(true)
            .build();

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ParameterProviderInitializationContext config) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PARAMETER_GROUP_DIRECTORIES);
        properties.add(PARAMETER_VALUE_BYTE_LIMIT);
        properties.add(PARAMETER_VALUE_ENCODING);

        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public List<ParameterGroup> fetchParameters(final ConfigurationContext context) {

        final List<ParameterGroup> parameterGroups = new ArrayList<>();

        final Collection<File> groupDirectories = getDirectories(context, PARAMETER_GROUP_DIRECTORIES);
        groupDirectories.forEach(directory -> {
            parameterGroups.add(getParameterGroup(context, directory, directory.getName()));
        });
        final AtomicInteger groupedParameterCount = new AtomicInteger(0);
        final Collection<String> groupNames = new HashSet<>();
        parameterGroups.forEach(group -> {
            groupedParameterCount.addAndGet(group.getParameters().size());
            groupNames.add(group.getGroupName());
        });
        getLogger().info("Fetched {} parameters.  Group names: {}", groupedParameterCount.get(), groupNames);
        return parameterGroups;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            final List<ParameterGroup> parameterGroups = fetchParameters(context);
            final long parameterCount = parameterGroups.stream()
                    .flatMap(group -> group.getParameters().stream())
                    .count();
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .verificationStepName("Fetch Parameters")
                    .explanation(String.format("Fetched %s files as parameters.", parameterCount))
                    .build());
        } catch (final IllegalArgumentException e) {
            verificationLogger.error("Failed to fetch parameters", e);
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .verificationStepName("Fetch Parameters")
                    .explanation("Failed to fetch parameters: " + e.getMessage())
                    .build());
        }
        return results;
    }

    private String getParameterValue(final String rawValue, final ParameterValueEncoding encoding) {
        if (ParameterValueEncoding.BASE64 == encoding) {
            return new String(Base64.getDecoder().decode(rawValue.getBytes(DEFAULT_CHARSET)), DEFAULT_CHARSET);
        }
        return rawValue;
    }

    private Collection<File> getDirectories(final ConfigurationContext context, final PropertyDescriptor descriptor) {
        return context.getProperty(descriptor).isSet()
                ? Arrays.stream(context.getProperty(descriptor).getValue().split(",")).map(String::trim).map(File::new).collect(Collectors.toSet())
                : Collections.emptySet();
    }

    private ParameterGroup getParameterGroup(final ConfigurationContext context, final File directory, final String groupName) {
        final int parameterSizeLimit = context.getProperty(PARAMETER_VALUE_BYTE_LIMIT).asDataSize(DataUnit.B).intValue();
        final ParameterValueEncoding parameterEncoding = ParameterValueEncoding.valueOf(context.getProperty(PARAMETER_VALUE_ENCODING).getValue().toUpperCase());

        final File[] files = directory.listFiles();

        final List<Parameter> parameters = new ArrayList<>();
        for (final File file : files) {
            if (file.isDirectory() || file.isHidden()) {
                continue;
            }

            final String parameterName = file.getName();

            try (final InputStream in = new BufferedInputStream(new LimitingInputStream(new FileInputStream(file), parameterSizeLimit))) {
                final String rawValue = IOUtils.toString(in, Charset.defaultCharset()).trim();
                final String parameterValue = getParameterValue(rawValue, parameterEncoding);

                if (parameterValue.length() >= parameterSizeLimit) {
                    getLogger().warn("Parameter {} may be truncated at {} bytes", parameterName, parameterValue.length());
                }

                parameters.add(new Parameter.Builder()
                    .name(parameterName)
                    .value(parameterValue)
                    .provided(true)
                    .build());
            } catch (final IOException e) {
                throw new RuntimeException(String.format("Failed to read file [%s]", file), e);
            }
        }
        return new ParameterGroup(groupName, parameters);
    }

    public static class MultiDirectoryExistsValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            String reason = null;
            if (value == null) {
                reason = "At least one directory is required";
            } else {
                final Set<String> directories = Arrays.stream(value.split(","))
                        .map(String::trim)
                        .collect(Collectors.toSet());
                try {
                    for (final String directory : directories) {
                        final File file = new File(directory);
                        if (!file.exists()) {
                            reason = "Directory " + file.getName() + " does not exist";
                        } else if (!file.isDirectory()) {
                            reason = "Path " + file.getName() + " does not point to a directory";
                        }
                    }
                } catch (final Exception e) {
                    reason = "Value is not a valid directory name";
                }
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    }
}
