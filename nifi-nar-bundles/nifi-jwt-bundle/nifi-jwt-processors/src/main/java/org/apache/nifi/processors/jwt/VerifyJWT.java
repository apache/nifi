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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.ValidationContext;
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
import org.bouncycastle.util.io.pem.PemReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.security.Security;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Collection;
import java.util.Map.Entry;

@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({ "jwt", "json", "flowfile", "unsign", "verify" })
@CapabilityDescription("Validates and unsigns a JWT either from the flowfile body or from an attribute. The JWT is unsigned "
        + "using a directory of public RSA keys specified by the \"Approved Public Keys\" property or by adding public keys "
        + "in ASCII format as extra properties. By default the processor will read the JWT from the content of the flowfile "
        + "and rewrite the contents with the JWT claims in JSON format. If an attribute name is given as the \"JWT Attribute "
        + "Name\" property then the content of that attribute will be unsigned and the JWT claims will be written as \"jwt.XXX\" attributes.")
@WritesAttributes({
        @WritesAttribute(attribute = "jwt.header", description = "If the flow file is routed to the success relationship "
                + "the attribute will contain the the header of the JWT."),
        @WritesAttribute(attribute = "jwt.footer", description = "If the flow file is routed to the success relationship "
                + "the attribute will contain the footer of the JWT."),
        @WritesAttribute(attribute = "jwt.XXX", description = "If the flow file is routed to the success relationship "
                + "and the JWT was unsugned from an attribute, these attributes will contain the claims of the JWT."),
        @WritesAttribute(attribute = "failure.reason", description = "If the flow file is routed to the failure relationship "
                + "the attribute will contain the error message resulting from the validation failure.") })
@SeeAlso({ SignJWT.class })
public class VerifyJWT extends AbstractProcessor {

    public static final PropertyDescriptor PUBLIC_KEYS_PATH = new PropertyDescriptor.Builder().name("PUBLIC_KEYS_PATH")
            .displayName("Approved Public Keys")
            .description(
                    "Path to the directory containing approved public certificates. Certificates should be x509 PEM files "
                            + "and match the RegExp configured in the \"Public Key Extension RegEx\" property. Can be left "
                            + "blank if all public keys are added as additional properties")
            .required(false).addValidator(CustomValidators.DIRECTORY_HAS_PUBLIC_KEYS_VALIDATOR).defaultValue("")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();

    public static final PropertyDescriptor PUBLIC_KEY_EXTENSION = new PropertyDescriptor.Builder()
            .name("PUBLIC_KEY_EXTENSION").displayName("Public Key Extension RegEx")
            .description(
                    "RegEx matching the extension(s) for the public keys in the \"Approved Public Keys\" directory")
            .required(true).defaultValue(".*\\.pub")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_WITH_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();

    public static final PropertyDescriptor JWT_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("JWT_ATTRIBUTE_NAME").displayName("JWT Attribute name")
            .description(
                    "The name of the attribute which contains the JWT. Leave empty to read JWT from the flowfile content")
            .required(false).defaultValue("").addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

    public static final Relationship SUCCESS_REL = new Relationship.Builder().name("success")
            .description("A FlowFile is routed to this relationship after its JWT has been validated and unsigned")
            .build();

    public static final Relationship FAILURE_REL = new Relationship.Builder().name("failure").description(
            "A FlowFile is routed to this relationship if the JWT cannot be validated or unsigned for any reason")
            .build();

    public static final String HEADER_ATTRIBUTE = "jwt.header";
    public static final String FOOTER_ATTRIBUTE = "jwt.footer";
    public static final String JWT_PREFIX_ATTRIBUTE = "jwt.";

    public static final String PUBLIC_KEY_PREFIX = "-----BEGIN PUBLIC KEY-----\n";
    public static final String PUBLIC_KEY_SUFFIX = "-----END PUBLIC KEY-----";
    public static final String SSH_RSA_PREFIX = "ssh-rsa ";

    private Set<PublicKey> keys;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private volatile Set<String> dynamicPropertyNames = new HashSet<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PUBLIC_KEYS_PATH);
        descriptors.add(PUBLIC_KEY_EXTENSION);
        descriptors.add(JWT_ATTRIBUTE_NAME);
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
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder().required(false).name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR).dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor property, final String oldValue, final String newValue) {
        final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
        if (newValue == null) {
            newDynamicPropertyNames.remove(property.getName());
        } else if (oldValue == null) { // new property
            newDynamicPropertyNames.add(property.getName());
        }
        this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> ret = new HashSet<>();
        final KeyFactory kf;
        try {
            kf = KeyFactory.getInstance("RSA", "BC");
        } catch (final NoSuchAlgorithmException | NoSuchProviderException e) {
            getLogger().error(e.getMessage());
            final ValidationResult result = new ValidationResult.Builder().valid(false).build();
            ret.add(result);
            return ret;
        }

        // read extension regex from property
        final String regex = context.getProperty(PUBLIC_KEY_EXTENSION).evaluateAttributeExpressions().getValue();

        // read public cert from property
        final String publicCertsString = context.getProperty(PUBLIC_KEYS_PATH).evaluateAttributeExpressions()
                .getValue();

        keys = new HashSet<>();
        // parse all public keys in directory
        if (!publicCertsString.isEmpty()) {
            final File publicCerts = new File(publicCertsString);
            for (final File cert : publicCerts.listFiles((dir, name) -> name.toLowerCase().matches(regex))) {
                try (PemReader pemReader = new PemReader(new InputStreamReader(new FileInputStream(cert)));) {
                    // read base64 encoded public key from file, process it and generate PublicKey
                    final byte[] keyBytes = Files.readAllBytes(cert.toPath());
                    final String keyString = new String(keyBytes);
                    if (!keyString.startsWith(PUBLIC_KEY_PREFIX)) {
                        if (keyString.startsWith(SSH_RSA_PREFIX)) {
                            final String command = "ssh-keygen -f " + cert.getName() + " -e -m pkcs8 > "
                                    + cert.getName();
                            getLogger().warn("Key \"" + cert.getAbsolutePath()
                                    + "\" is not in a usable format. Use the command '" + command
                                    + "' to convert it to a PEM format.");
                        } else {
                            getLogger().warn("Key \"" + cert.getAbsolutePath()
                                    + "\" is not in a usable format. Public keys must "
                                    + "be a x509 encoded PEM format.");
                        }
                        continue;
                    }
                    final byte[] content = pemReader.readPemObject().getContent();
                    final X509EncodedKeySpec spec = new X509EncodedKeySpec(content);
                    final PublicKey publicKey = kf.generatePublic(spec);
                    keys.add(publicKey);
                } catch (final Exception ex) {
                    getLogger().warn("Error parsing private key \"" + cert.getAbsolutePath() + "\" : " + ex);
                }
            }
        }

        // parse extra keys which may have been added as properties
        final Map<PropertyDescriptor, String> extraKeys = context.getProperties();
        for (final Entry<PropertyDescriptor, String> keyProp : extraKeys.entrySet()) {
            final PropertyDescriptor prop = keyProp.getKey();
            if (prop == PUBLIC_KEYS_PATH || prop == PUBLIC_KEY_EXTENSION || prop == JWT_ATTRIBUTE_NAME) {
                // Expected properties, are not keys so disregard
                continue;
            }
            final String key = context.getProperty(prop).evaluateAttributeExpressions().getValue();
            try (PemReader pemReader = new PemReader(
                    new InputStreamReader(new ByteArrayInputStream(key.getBytes())));) {
                final byte[] content = pemReader.readPemObject().getContent();
                final X509EncodedKeySpec spec = new X509EncodedKeySpec(content);
                final PublicKey publicKey = kf.generatePublic(spec);
                keys.add(publicKey);
            } catch (final Exception ex) {
                final ValidationResult result = new ValidationResult.Builder()
                        .subject(keyProp.getKey().getDisplayName()).input(key).explanation("key could not be parsed")
                        .valid(false).build();
                ret.add(result);
            }
        }
        if (keys.isEmpty()) {
            final ValidationResult result = new ValidationResult.Builder().subject("All properties").input("all")
                    .explanation(
                            "no public keys have been read. Please enter valid public keys as properties or to the filesystem.")
                    .valid(false).build();
            ret.add(result);
        }
        return ret;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // read attribute from property
        boolean jwtInBody = true;
        final String jwtAttribute = context.getProperty(JWT_ATTRIBUTE_NAME).evaluateAttributeExpressions(flowFile)
                .getValue();
        if (!"".equals(jwtAttribute)) {
            jwtInBody = false;
        }

        String jwt;
        if (jwtInBody) {
            // read JWT from flowfile
            try (InputStream is = session.read(flowFile);) {
                jwt = IOUtils.toString(is, "UTF-8");
            } catch (final Exception e) {
                // cannot read flowfile so transfer to failure later by null
                jwt = null;
            }
        } else {
            jwt = flowFile.getAttribute(jwtAttribute);
        }

        for (final PublicKey publicKey : keys) {
            try {
                // Verify and unsign JWT
                final Jws<Claims> jws = Jwts.parserBuilder().setSigningKey(publicKey).build().parseClaimsJws(jwt);
                final String json = new ObjectMapper().writeValueAsString(jws.getBody());
                final String header = jwt.split("\\.")[0];
                final String footer = jwt.split("\\.")[2];

                // Output results
                flowFile = session.putAttribute(flowFile, HEADER_ATTRIBUTE, header);
                flowFile = session.putAttribute(flowFile, FOOTER_ATTRIBUTE, footer);
                if (jwtInBody) {
                    flowFile = session.write(flowFile, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream out) throws IOException {
                            out.write(json.getBytes());
                        }
                    });
                    session.getProvenanceReporter().modifyContent(flowFile);
                    session.getProvenanceReporter().route(flowFile, SUCCESS_REL);
                    session.transfer(flowFile, SUCCESS_REL);
                } else {
                    final Claims body = jws.getBody();
                    for (final Entry<String, Object> entry : body.entrySet()) {
                        flowFile = session.putAttribute(flowFile, JWT_PREFIX_ATTRIBUTE + entry.getKey(),
                                entry.getValue().toString());
                    }
                    session.transfer(flowFile, SUCCESS_REL);
                }
                return;
            } catch (final JwtException ex) {
                // expect these for certs that do not verify the jwt claims
            } catch (final Exception ex) {
                getLogger().warn("Error during verifying JWT: " + ex);
            }
        }
        if (jwt == null) {
            flowFile = session.putAttribute(flowFile, "failure.reason", "no JWT found");
        } else {
            flowFile = session.putAttribute(flowFile, "failure.reason", "unable to verify and unsign JWT");
        }
        session.getProvenanceReporter().route(flowFile, FAILURE_REL);
        session.transfer(flowFile, FAILURE_REL);
    }
}