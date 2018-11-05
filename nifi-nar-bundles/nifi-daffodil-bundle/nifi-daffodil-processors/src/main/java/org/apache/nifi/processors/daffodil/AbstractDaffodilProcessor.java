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

package org.apache.nifi.processors.daffodil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.daffodil.japi.Compiler;
import org.apache.daffodil.japi.Daffodil;
import org.apache.daffodil.japi.DataProcessor;
import org.apache.daffodil.japi.Diagnostic;
import org.apache.daffodil.japi.InvalidParserException;
import org.apache.daffodil.japi.InvalidUsageException;
import org.apache.daffodil.japi.ProcessorFactory;
import org.apache.daffodil.japi.ValidationMode;
import org.apache.daffodil.japi.WithDiagnostics;


public abstract class AbstractDaffodilProcessor extends AbstractProcessor {

    abstract protected void processWithDaffodil(final DataProcessor dp, final FlowFile ff, final InputStream in, final OutputStream out, String infosetType) throws IOException;

    /**
     * Returns the mime type of the resulting output FlowFile. If the mime type
     * cannot be determine, this should return null, and the mime.type
     * attribute will be removed from the output FlowFile.
     */
    abstract protected String getOutputMimeType(String infosetType);
    abstract protected boolean isUnparse();

    public static final PropertyDescriptor DFDL_SCHEMA_FILE = new PropertyDescriptor.Builder()
            .name("dfdl-schema-file")
            .displayName("DFDL Schema File")
            .description("Full path to the DFDL schema file that is to be used for parsing/unparsing.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRE_COMPILED_SCHEMA = new PropertyDescriptor.Builder()
            .name("pre-compiled-schema")
            .displayName("Pre-compiled Schema")
            .description("Specify whether the DFDL Schema File is a pre-compiled schema that can be reloaded or if it is an XML schema that needs to be compiled. Set to true if it is pre-compiled.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final String XML_MIME_TYPE = "application/xml";
    static final String JSON_MIME_TYPE = "application/json";

    static final String XML_VALUE = "xml";
    static final String JSON_VALUE = "json";
    static final String ATTRIBUTE_VALUE = "use mime.type attribute";

    static final AllowableValue INFOSET_TYPE_XML = new AllowableValue(XML_VALUE, XML_VALUE, "The FlowFile representation is XML");
    static final AllowableValue INFOSET_TYPE_JSON = new AllowableValue(JSON_VALUE, JSON_VALUE, "The FlowFile representation is JSON");
    static final AllowableValue INFOSET_TYPE_ATTRIBUTE = new AllowableValue(ATTRIBUTE_VALUE, ATTRIBUTE_VALUE, "The FlowFile representation is determined based on the mime.type attribute");

    public static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("cache-size")
            .displayName("Cache Size")
            .description("Maximum number of compiled DFDL schemas to cache. Zero disables the cache.")
            .required(true)
            .defaultValue("50")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_TTL_AFTER_LAST_ACCESS = new PropertyDescriptor.Builder()
            .name("cache-ttl-after-last-access")
            .displayName("Cache TTL After Last Access")
            .description("The cache TTL (time-to-live) or how long to keep compiled DFDL schemas in the cache after last access.")
            .required(true)
            .defaultValue("30 mins")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final String OFF_VALUE = "off";
    static final String LIMITED_VALUE = "limited";
    static final String FULL_VALUE = "full";

    static final AllowableValue VALIDATION_MODE_OFF = new AllowableValue(OFF_VALUE, OFF_VALUE, "Disable infoset validation");
    static final AllowableValue VALIDATION_MODE_LIMITED= new AllowableValue(LIMITED_VALUE, LIMITED_VALUE, "Facet/restriction validation using Daffodil");
    static final AllowableValue VALIDATION_MODE_FULL = new AllowableValue(FULL_VALUE, FULL_VALUE, "Full schema validation using Xerces");

    public static final PropertyDescriptor VALIDATION_MODE = new PropertyDescriptor.Builder()
            .name("validation-mode")
            .displayName("Validation Mode")
            .description("The type of validation to be performed on the infoset.")
            .required(true)
            .defaultValue(OFF_VALUE)
            .allowableValues(VALIDATION_MODE_OFF, VALIDATION_MODE_LIMITED, VALIDATION_MODE_FULL)
            .build();

    /**
     * This is not static like the other PropertyDescriptors. This is because
     * the allowable values differ based on whether this is parse or unparse
     * (mime.type attribute is not allow for parse). So on init() we will
     * create this property descriptor accordingly.
     */
    private PropertyDescriptor INFOSET_TYPE = null;


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("When a parse/unparse is successful, the resulting FlowFile is routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("When a parse/unparse fails, it will be routed to this relationship")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        List<AllowableValue> allowableInfosetTypeValues = new ArrayList(Arrays.asList(INFOSET_TYPE_XML, INFOSET_TYPE_JSON));
        if (isUnparse()) {
            // using the mime type for infoset type only applies to unparse
            allowableInfosetTypeValues.add(INFOSET_TYPE_ATTRIBUTE);
        }

        INFOSET_TYPE = new PropertyDescriptor.Builder()
            .name("infoset-type")
            .displayName("Infoset Type")
            .description("The format of the FlowFile to output (for parsing) or input (for unparsing).")
            .required(true)
            .defaultValue(INFOSET_TYPE_XML.getValue())
            .allowableValues(allowableInfosetTypeValues.toArray(new AllowableValue[allowableInfosetTypeValues.size()]))
            .build();

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DFDL_SCHEMA_FILE);
        properties.add(PRE_COMPILED_SCHEMA);
        properties.add(INFOSET_TYPE);
        properties.add(VALIDATION_MODE);
        properties.add(CACHE_SIZE);
        properties.add(CACHE_TTL_AFTER_LAST_ACCESS);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private LoadingCache<CacheKey, DataProcessor> cache;

    static class CacheKey {
        public String dfdlSchema;
        public Boolean preCompiled;

        public CacheKey(String dfdlSchema, Boolean preCompiled) {
            this.dfdlSchema = dfdlSchema;
            this.preCompiled = preCompiled;
        }

        public int hashCode() {
          return Objects.hash(dfdlSchema, preCompiled);
        }

        public boolean equals(Object obj) {
          if (!(obj instanceof CacheKey)) return false;
          if (obj == this) return true;

          CacheKey that = (CacheKey)obj;
          return Objects.equals(this.dfdlSchema, that.dfdlSchema) && Objects.equals(this.preCompiled, that.preCompiled);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    private DataProcessor newDaffodilDataProcessor(String dfdlSchema, Boolean preCompiled) throws IOException {
        File f = new File(dfdlSchema);
        Compiler c = Daffodil.compiler();
        DataProcessor dp;
        if (preCompiled) {
            try {
                dp = c.reload(f);
            } catch (InvalidParserException e) {
                getLogger().error("Failed to reload pre-compiled DFDL schema: " + dfdlSchema + ". " + e.getMessage());
                throw new DaffodilCompileException("Failed to reload pre-compiled DFDL schema: " + dfdlSchema + ". " + e.getMessage());
            }
        } else {
            ProcessorFactory pf = c.compileFile(f);
            if (pf.isError()) {
                getLogger().error("Failed to compile DFDL schema: " + dfdlSchema);
                logDiagnostics(pf);
                throw new DaffodilCompileException("Failed to compile DFDL schema: " + dfdlSchema);
            }
            dp = pf.onPath("/");
            if (dp.isError()) {
                getLogger().error("Failed to compile DFDL schema: " + dfdlSchema);
                logDiagnostics(dp);
                throw new DaffodilCompileException("Failed to compile DFDL schema: " + dfdlSchema);
            }
        }
        return dp;
    }

    protected DataProcessor getDataProcessor(String dfdlSchema, Boolean preCompiled) throws IOException {
        if (cache != null) {
            try {
                return cache.get(new CacheKey(dfdlSchema, preCompiled));
            } catch (ExecutionException e) {
                throw new IOException(e);
            }
        } else {
            return newDaffodilDataProcessor(dfdlSchema, preCompiled);
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final ComponentLog logger = getLogger();
        final Integer cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final Long cacheTTL = context.getProperty(CACHE_TTL_AFTER_LAST_ACCESS).asTimePeriod(TimeUnit.SECONDS);

        if (cacheSize > 0) {
            CacheBuilder cacheBuilder = CacheBuilder.newBuilder().maximumSize(cacheSize);
            if (cacheTTL > 0) {
                cacheBuilder = cacheBuilder.expireAfterAccess(cacheTTL, TimeUnit.SECONDS);
            }

            cache = cacheBuilder.build(
               new CacheLoader<CacheKey, DataProcessor>() {
                   public DataProcessor load(CacheKey key) throws IOException {
                       return newDaffodilDataProcessor(key.dfdlSchema, key.preCompiled);
                   }
               });
        } else {
            cache = null;
            logger.warn("Daffodil data processor cache disabled because cache size is set to 0.");
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final StopWatch stopWatch = new StopWatch(true);
        final String dfdlSchema = context.getProperty(DFDL_SCHEMA_FILE).evaluateAttributeExpressions(original).getValue();
        final Boolean preCompiled = context.getProperty(PRE_COMPILED_SCHEMA).evaluateAttributeExpressions(original).asBoolean();
        String infosetTypeValue = context.getProperty(INFOSET_TYPE).getValue();
        final String infosetType;
        final ValidationMode validationMode;

        switch (context.getProperty(VALIDATION_MODE).getValue()) {
            case OFF_VALUE: validationMode = ValidationMode.Off; break;
            case LIMITED_VALUE: validationMode = ValidationMode.Limited; break;
            case FULL_VALUE: validationMode = ValidationMode.Full; break;
            default: throw new AssertionError("validation mode was not one of 'off', 'limited', or 'full'");
        }

        if (infosetTypeValue.equals(ATTRIBUTE_VALUE)) {
            if (!isUnparse()) {
                throw new AssertionError("infoset type 'attribute' should only occur with Daffodil unparse");
             }

            String inputMimeType = original.getAttribute(CoreAttributes.MIME_TYPE.key());
            switch (inputMimeType == null ? "" : inputMimeType) {
                case XML_MIME_TYPE: infosetType = XML_VALUE; break;
                case JSON_MIME_TYPE: infosetType = JSON_VALUE; break;
                default:
                    logger.error("Infoset Type is 'attribute', but the mime.type attribute is not set or not recognized for {}.", new Object[]{original});
                    session.transfer(original, REL_FAILURE);
                    return;
            }
        } else {
            infosetType = infosetTypeValue;
        }

        try {
            FlowFile output = session.write(original, new StreamCallback() {
                @Override
                public void process(final InputStream in, final OutputStream out) throws IOException {
                    DataProcessor dp = getDataProcessor(dfdlSchema, preCompiled);
                    try {
                        dp.setValidationMode(validationMode);
                    } catch (InvalidUsageException e) {
                        throw new IOException(e);
                    }
                    processWithDaffodil(dp, original, in, out, infosetType);
                }
            });
            final String outputMimeType = getOutputMimeType(infosetType);
            if (outputMimeType != null) {
                output = session.putAttribute(output, CoreAttributes.MIME_TYPE.key(), outputMimeType);
            } else {
                output = session.removeAttribute(output, CoreAttributes.MIME_TYPE.key());
            }
            session.transfer(output, REL_SUCCESS);
            session.getProvenanceReporter().modifyContent(output, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            logger.debug("Processed {}", new Object[]{original});
        } catch (ProcessException e) {
            logger.error("Failed to process {} due to {}", new Object[]{original, e});
            session.transfer(original, REL_FAILURE);
        }
    }

    protected void logDiagnostics(WithDiagnostics withDiags) {
        final ComponentLog logger = getLogger();
        final List<Diagnostic> diags = withDiags.getDiagnostics();
        for (Diagnostic diag : diags) {
            String message = diag.toString();
            if (diag.isError()) {
                logger.error(message);
            } else {
                logger.warn(message);
            }
        }
    }
}
