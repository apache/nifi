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
package org.apache.nifi.processors.jwt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.KeyFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;

@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({ "jwt", "json", "flowfile", "sign" })
@CapabilityDescription("JWT Signs a JSON file with a specified private key. The JSON must be key value pairs. The key can be specified in the \"Private Key\" property "
        + "directly in ASCII format, or as a path to the key's location on the filesystem, depending on the \"Private Key Type\" property's value. The JWT will be "
        + "outputted in the content of the flowfile if successful.")
@WritesAttributes({
        @WritesAttribute(attribute = "failure.reason", description = "If the flow file is routed to the failure relationship "
                + "the attribute will contain the error message resulting from the validation failure.") })
@SeeAlso({ VerifyJWT.class })
public class SignJWT extends AbstractProcessor {

    public static final String PATH = "Path";
    public static final String ASCII = "ASCII";
    public static final String FAILURE_REASON_ATTR = "failure.reason";
    public static final String INVALID_JSON = "Invalid JSON: ";
    public static final String ERROR_PREFIX = "Error signing JWT: ";

    public static final PropertyDescriptor KEY_TYPE = new PropertyDescriptor.Builder().name("PRIVATE_KEY_TYPE")
            .displayName("Private Key Type")
            .description(
                    "Specify whether the \"Private Key\" property is the private key in ASCII format or the path of the private key "
                            + "on the filesystem")
            .required(true).allowableValues(PATH, ASCII).build();

    public static final PropertyDescriptor PRIVATE_KEY = new PropertyDescriptor.Builder().name("PRIVATE_KEY")
            .displayName("Private Key")
            .description(
                    "Private key to sign the JWT. Either the path to the key on the local filesystem or they ASCII format of the private "
                            + "key, as specified by the \"Private Key\" property.The key must be readable by the NiFi user")
            .required(true).addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();

    public static final Relationship SUCCESS_REL = new Relationship.Builder().name("success")
            .description("A FlowFile is routed to this relationship after its JWT has been validated and unsigned")
            .build();

    public static final Relationship FAILURE_REL = new Relationship.Builder().name("failure").description(
            "A FlowFile is routed to this relationship if the JWT cannot be validated or unsigned for any reason")
            .build();

    private PrivateKey privateKey;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(KEY_TYPE);
        descriptors.add(PRIVATE_KEY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_REL);
        relationships.add(FAILURE_REL);
        this.relationships = Collections.unmodifiableSet(relationships);

        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
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

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        InputStream is = null;
        final String privateKeyProperty = context.getProperty(PRIVATE_KEY).evaluateAttributeExpressions().getValue();

        if (context.getProperty(KEY_TYPE).getValue().equals(PATH)) {
            final File key = new File(privateKeyProperty);
            try {
                is = new FileInputStream(key);
            } catch (final FileNotFoundException e) {
                throw new ProcessException(e);
            }
        } else {
            is = new ByteArrayInputStream(privateKeyProperty.getBytes());
        }
        try (PemReader pemReader = new PemReader(new InputStreamReader(is));) {
            final KeyFactory factory = KeyFactory.getInstance("RSA", "BC");
            final PemObject pemObj = pemReader.readPemObject();
            if (null == pemObj) {
                throw new IOException("Not a valid PEM object");
            }
            final byte[] content = pemObj.getContent();
            final PKCS8EncodedKeySpec privKeySpec = new PKCS8EncodedKeySpec(content);
            privateKey = factory.generatePrivate(privKeySpec);
            final ValidationResult result = new ValidationResult.Builder().subject(PRIVATE_KEY.getDisplayName())
                    .input(privateKeyProperty).explanation("the private key is valid").valid(true).build();
            final Collection<ValidationResult> ret = new HashSet<>();
            ret.add(result);
            return ret;
        } catch (NoSuchAlgorithmException | NoSuchProviderException | IOException | InvalidKeySpecException e) {
            privateKey = null;
            final ValidationResult result = new ValidationResult.Builder().subject(PRIVATE_KEY.getDisplayName())
                    .input(privateKeyProperty).explanation("the private key cannot be parsed: " + e).valid(false)
                    .build();
            final Collection<ValidationResult> ret = new HashSet<>();
            ret.add(result);
            return ret;
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        // empty
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        if (privateKey == null) {
            session.rollback();
            context.yield();
            return;
        }

        try (InputStream json = session.read(flowFile)) {
            // Parse json into map to be JWT signed with the PrivateKey
            final Map<String, Object> claimsMap = new ObjectMapper().readValue(json, Map.class);
            final String jwt = Jwts.builder().setClaims(claimsMap).signWith(privateKey).compact();

            // Output results
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(jwt.getBytes());
                }
            });
            session.getProvenanceReporter().modifyContent(flowFile);
            session.getProvenanceReporter().route(flowFile, SUCCESS_REL);
            session.transfer(flowFile, SUCCESS_REL);
        } catch (final JsonProcessingException ex) {
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTR, INVALID_JSON + ex.getLocalizedMessage());
            session.getProvenanceReporter().route(flowFile, FAILURE_REL);
            session.transfer(flowFile, FAILURE_REL);
        } catch (JwtException | IOException ex) {
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTR, ERROR_PREFIX + ex.getLocalizedMessage());
            session.getProvenanceReporter().route(flowFile, FAILURE_REL);
            session.transfer(flowFile, FAILURE_REL);
        }
    }
}