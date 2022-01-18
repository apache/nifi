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
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Tags({"file"})
@CapabilityDescription("Fetches parameters from files.  Parameter groups are indicated by a set of directories, and files within the directories map to parameter names. " +
        "The content of the file becomes the parameter value.")

@DynamicProperties(
    @DynamicProperty(name = "Mapped parameter name", value = "Filename",
        description = "Maps a raw fetched parameter from the external system to the name of a parameter inside the dataflow")
)
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.READ_FILESYSTEM,
                        explanation = "Provides operator the ability to read from any file that NiFi has access to.")
        }
)
public class FileParameterProvider extends AbstractParameterProvider implements VerifiableParameterProvider {
    public static final PropertyDescriptor SENSITIVE_PARAMETER_REGEX = new PropertyDescriptor.Builder()
            .name("sensitive-parameter-regex")
            .displayName("Sensitive Parameter Regex")
            .description("A Regular Expression indicating which files to include as sensitive parameters.  Any that match this pattern " +
                    " are automatically excluded from the non-sensitive parameters.  If not specified, no sensitive parameters will be included.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor NON_SENSITIVE_PARAMETER_REGEX = new PropertyDescriptor.Builder()
            .name("non-sensitive-parameter-regex")
            .displayName("Non-Sensitive Parameter Regex")
            .description("A Regular Expression indicating which files to include as non-sensitive parameters.  If the Sensitive Parameter Regex " +
                    "is specified, filenames matching that Regex will be excluded from the non-sensitive parameters.  " +
                    "If not specified, no non-sensitive parameters will be included.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
    public static final PropertyDescriptor GROUPED_PARAMETER_DIRECTORIES = new PropertyDescriptor.Builder()
            .name("grouped-parameter-directories")
            .displayName("Grouped Parameter Directories")
            .description("A comma-separated list of directory absolute paths that will map to named parameter groups.  Each directory that contains " +
                    "files will map to a parameter group, named after the innermost directory in the path.  Files inside the directory will map to " +
                    "parameter names, whose values are the content of each respective file.")
            .addValidator(new MultiDirectoryExistsValidator())
            .build();
    public static final PropertyDescriptor UNGROUPED_PARAMETER_DIRECTORIES = new PropertyDescriptor.Builder()
            .name("ungrouped-parameter-directories")
            .displayName("Ungrouped Parameter Directories")
            .description("A comma-separated list of directory absolute paths that will map to ungrouped parameters.  Any directory in this list that contains " +
                    "files will map to a set of ungrouped parameters, which may apply to any referencing Parameter Context.  Files inside the directory will map to " +
                    "parameter names, whose values are the content of each respective file.")
            .addValidator(new MultiDirectoryExistsValidator())
            .build();
    public static final PropertyDescriptor PARAMETER_VALUE_BYTE_LIMIT = new PropertyDescriptor.Builder()
            .name("parameter-value-byte-limit")
            .displayName("Parameter Value Byte Limit")
            .description("The maximum byte size of a parameter value.  Since parameter values are pulled from the contents of files, this is a safeguard that can " +
                    "prevent memory issues if large files are included.")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, 1024))
            .defaultValue("256 B")
            .required(true)
            .build();

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ParameterProviderInitializationContext config) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SENSITIVE_PARAMETER_REGEX);
        properties.add(NON_SENSITIVE_PARAMETER_REGEX);
        properties.add(GROUPED_PARAMETER_DIRECTORIES);
        properties.add(UNGROUPED_PARAMETER_DIRECTORIES);
        properties.add(PARAMETER_VALUE_BYTE_LIMIT);

        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> validationResults = new ArrayList<>();

        if (!context.getProperty(GROUPED_PARAMETER_DIRECTORIES).isSet() && !context.getProperty(UNGROUPED_PARAMETER_DIRECTORIES).isSet()) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(GROUPED_PARAMETER_DIRECTORIES.getDisplayName())
                    .explanation(String.format("at least one of '%s' or '%s' is required", GROUPED_PARAMETER_DIRECTORIES.getDisplayName(), UNGROUPED_PARAMETER_DIRECTORIES.getDisplayName()))
                    .valid(false)
                    .build());
        }

        return validationResults;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public List<ProvidedParameterGroup> fetchParameters(final ConfigurationContext context) {

        final List<ProvidedParameterGroup> parameterGroups = new ArrayList<>();

        final Collection<File> groupedDirectories = getDirectories(context, GROUPED_PARAMETER_DIRECTORIES);
        final Collection<File> ungroupedDirectories = getDirectories(context, UNGROUPED_PARAMETER_DIRECTORIES);
        groupedDirectories.forEach(directory -> {
            parameterGroups.addAll(getParameters(context, directory, directory.getName()));
        });
        ungroupedDirectories.forEach(directory -> {
            parameterGroups.addAll(getParameters(context, directory, null));
        });
        final AtomicInteger ungroupedParameterCount = new AtomicInteger(0);
        final AtomicInteger groupedParameterCount = new AtomicInteger(0);
        final Collection<String> groupNames = new HashSet<>();
        parameterGroups.forEach(group -> {
            if (group.getGroupKey().getGroupName() == null) {
                ungroupedParameterCount.addAndGet(group.getItems().size());
            } else {
                groupedParameterCount.addAndGet(group.getItems().size());
                groupNames.add(group.getGroupKey().getGroupName());
            }
        });
        getLogger().info("Fetched {} ungrouped parameters and {} grouped parameters.  Group names: {}", ungroupedParameterCount.get(), groupedParameterCount.get(), groupNames);
        return parameterGroups;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            final List<ProvidedParameterGroup> parameterGroups = fetchParameters(context);
            int sensitiveCount = 0;
            int nonSensitiveCount = 0;
            for (final ProvidedParameterGroup group : parameterGroups) {
                if (group.getGroupKey().getSensitivity() == ParameterSensitivity.SENSITIVE) {
                    sensitiveCount += group.getItems().size();
                }
                if (group.getGroupKey().getSensitivity() == ParameterSensitivity.NON_SENSITIVE) {
                    nonSensitiveCount += group.getItems().size();
                }
            }
            final Set<String> parameterGroupNames = parameterGroups.stream().map(group -> group.getGroupKey().getGroupName() == null ? "<any>" : group.getGroupKey().getGroupName())
                    .collect(Collectors.toSet());
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .verificationStepName("Fetch Parameters")
                    .explanation(String.format("Fetched %s files as sensitive parameters and %s files as non-sensitive parameters.  Matching parameter contexts may be named: %s",
                            sensitiveCount, nonSensitiveCount, parameterGroupNames))
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

    private Collection<File> getDirectories(final ConfigurationContext context, final PropertyDescriptor descriptor) {
        return context.getProperty(descriptor).isSet()
                ? Arrays.stream(context.getProperty(descriptor).getValue().split(",")).map(String::trim).map(File::new) .collect(Collectors.toSet())
                : Collections.emptySet();
    }

    private List<ProvidedParameterGroup> getParameters(final ConfigurationContext context, final File directory, final String groupName) {
        final List<ProvidedParameterGroup> parameterGroups = new ArrayList<>();
        final Pattern sensitivePattern = context.getProperty(SENSITIVE_PARAMETER_REGEX).isSet()
                ? Pattern.compile(context.getProperty(SENSITIVE_PARAMETER_REGEX).getValue()) : null;
        final Pattern nonSensitivePattern = context.getProperty(NON_SENSITIVE_PARAMETER_REGEX).isSet()
                ? Pattern.compile(context.getProperty(NON_SENSITIVE_PARAMETER_REGEX).getValue()) : null;
        final int parameterSizeLimit = context.getProperty(PARAMETER_VALUE_BYTE_LIMIT).asDataSize(DataUnit.B).intValue();

        final File[] files = directory.listFiles((dir, name) ->
                (sensitivePattern == null || sensitivePattern.matcher(name).matches() || nonSensitivePattern == null || nonSensitivePattern.matcher(name).matches()));

        final List<Parameter> sensitiveParameters = new ArrayList<>();
        final List<Parameter> nonSensitiveParameters = new ArrayList<>();
        for (final File file : files) {
            if (file.isDirectory() || file.isHidden()) {
                continue;
            }

            final String parameterName = file.getName();

            try (final InputStream in = new BufferedInputStream(new LimitingInputStream(new FileInputStream(file), parameterSizeLimit))) {
                final String parameterValue = IOUtils.toString(in, Charset.defaultCharset()).trim();
                if (parameterValue.length() >= parameterSizeLimit) {
                    getLogger().warn("Parameter {} may be truncated at {} bytes", parameterName, parameterValue.length());
                }

                final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder().name(parameterName).build();
                final Parameter parameter = new Parameter(parameterDescriptor, parameterValue, null, true);
                if (sensitivePattern != null && sensitivePattern.matcher(parameterName).matches()) {
                    sensitiveParameters.add(parameter);
                } else if (nonSensitivePattern != null && nonSensitivePattern.matcher(parameterName).matches()) {
                    nonSensitiveParameters.add(parameter);
                }
            } catch (final IOException e) {
                getLogger().error("Failed to read file [{}]", new Object[] { file }, e);
                throw new RuntimeException(String.format("Failed to read file [%s]", file), e);
            }
        }
        parameterGroups.add(new ProvidedParameterGroup(groupName, ParameterSensitivity.SENSITIVE, sensitiveParameters));
        parameterGroups.add(new ProvidedParameterGroup(groupName, ParameterSensitivity.NON_SENSITIVE, nonSensitiveParameters));
        return parameterGroups;
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
