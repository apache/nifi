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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.hadoop.HadoopValidators;
import org.apache.nifi.util.StringUtils;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.SchemaNotFoundException;
import org.kitesdk.data.URIBuilder;
import org.kitesdk.data.spi.DefaultConfiguration;

abstract class AbstractKiteProcessor extends AbstractProcessor {

    private static final Splitter COMMA = Splitter.on(',').trimResults();

    protected static final PropertyDescriptor CONF_XML_FILES
            = new PropertyDescriptor.Builder()
            .name("Hadoop configuration files")
            .displayName("Hadoop configuration Resources")
            .description("A file or comma separated list of files which contains the Hadoop file system configuration. Without this, Hadoop "
                    + "will search the classpath for a 'core-site.xml' and 'hdfs-site.xml' file or will revert to a default configuration.")
            .required(false)
            .addValidator(HadoopValidators.ONE_OR_MORE_FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

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

        if(uri.getScheme() == null) {
            throw new SchemaNotFoundException("If the schema is not a JSON string, a scheme must be specified in the URI "
                    + "(ex: dataset:, view:, resource:, file:, hdfs:, etc).");
        }

        try {
            if ("dataset".equals(uri.getScheme()) || "view".equals(uri.getScheme())) {
                return Datasets.load(uri).getDataset().getDescriptor().getSchema();
            } else if ("resource".equals(uri.getScheme())) {
                try (InputStream in = Resources.getResource(uri.getSchemeSpecificPart()).openStream()) {
                    return parseSchema(uri, in);
                }
            } else {
                // try to open the file
                Path schemaPath = new Path(uri);
                try (FileSystem fs = schemaPath.getFileSystem(conf); InputStream in = fs.open(schemaPath)) {
                    return parseSchema(uri, in);
                }
            }

        } catch (DatasetNotFoundException e) {
            throw new SchemaNotFoundException("Cannot read schema of missing dataset: " + uri, e);
        } catch (IOException e) {
            throw new SchemaNotFoundException("Failed while reading " + uri + ": " + e.getMessage(), e);
        }
    }

    private static Schema parseSchema(String literal) {
        try {
            return new Schema.Parser().parse(literal);
        } catch (RuntimeException e) {
            throw new SchemaNotFoundException("Failed to parse schema: " + literal, e);
        }
    }

    private static Schema parseSchema(URI uri, InputStream in) throws IOException {
        try {
            return new Schema.Parser().parse(in);
        } catch (RuntimeException e) {
            throw new SchemaNotFoundException("Failed to parse schema at " + uri, e);
        }
    }

    protected static final Validator SCHEMA_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String uri, ValidationContext context) {
            Configuration conf = getConfiguration(context.getProperty(CONF_XML_FILES).evaluateAttributeExpressions().getValue());
            String error = null;

            if(StringUtils.isBlank(uri)) {
                return new ValidationResult.Builder().subject(subject).input(uri).explanation("Schema cannot be null.").valid(false).build();
            }

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

    protected static final List<PropertyDescriptor> ABSTRACT_KITE_PROPS = ImmutableList.<PropertyDescriptor>builder()
            .add(CONF_XML_FILES)
            .build();

    static List<PropertyDescriptor> getProperties() {
        return ABSTRACT_KITE_PROPS;
    }

    @OnScheduled
    protected void setDefaultConfiguration(ProcessContext context)
            throws IOException {
        DefaultConfiguration.set(getConfiguration(
                context.getProperty(CONF_XML_FILES).evaluateAttributeExpressions().getValue()));
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

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ABSTRACT_KITE_PROPS;
    }
}
