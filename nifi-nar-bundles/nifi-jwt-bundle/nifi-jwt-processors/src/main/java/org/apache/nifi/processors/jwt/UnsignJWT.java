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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.bouncycastle.util.encoders.Base64;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import java.util.Map.Entry;

@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({ "jwt", "json", "flowfile", "unsign" })
@CapabilityDescription("Validates and unsigns a JWT either from the flowfile body or from an attribute. The JWT is unsigned "
                + "using a directory of public RSA keys specified by the \"Approved Public Keys\" property. By default the "
                + "processor will read the JWT from the content of the flowfile and rewrite the contents with the JWT claims "
                + "in JSON format. If an attribute name is given as the \"JWT Attribute Name\" property then the content of "
                + "that attribute will be unsigned and the JWT claims will be written as \"jwt.XXX\" attributes.")
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
public class UnsignJWT extends AbstractProcessor {

    public static final PropertyDescriptor PUBLIC_KEYS_PATH = new PropertyDescriptor.Builder().name("PUBLIC_KEYS_PATH")
            .displayName("Approved Public Keys")
            .description("Path to the directory containing approved public certificates. Certificates are expected to have "
                + "extension \".pub\" and be x509 PEM files")
            .required(true).addValidator(CustomValidators.DIRECTORY_HAS_PUBLIC_KEYS_VALIDATOR).build();

    public static final PropertyDescriptor JWT_ATTRIBUTE_NAME = new PropertyDescriptor.Builder().name("JWT_ATTRIBUTE_NAME")
            .displayName("JWT Attribute name")
            .description("The name of the attribute which contains the JWT. Leave empty to read JWT from the flowfile content")
            .required(false).defaultValue("").addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR).build();

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

    private File publicCerts = null;
    private boolean jwtInBody = true;
    private String jwtAttribute;
    private KeyFactory kf;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PUBLIC_KEYS_PATH);
        descriptors.add(JWT_ATTRIBUTE_NAME);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_REL);
        relationships.add(FAILURE_REL);
        this.relationships = Collections.unmodifiableSet(relationships);
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
        // read public cert from property
        publicCerts = new File(context.getProperty(PUBLIC_KEYS_PATH).getValue());
        try {
            kf = KeyFactory.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
            context.yield();
        }

        // read attribute from property
        jwtAttribute = context.getProperty(JWT_ATTRIBUTE_NAME).getValue();
        if (!"".equals(jwtAttribute)) {
            jwtInBody = false;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String jwt;
        if (jwtInBody) {
            // read JWT from flowfile
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            session.exportTo(flowFile, bytes);
            jwt = bytes.toString();
        } else {
            jwt = flowFile.getAttribute(jwtAttribute);
        }

        for (final File cert : publicCerts.listFiles((dir, name) -> name.toLowerCase().endsWith(".pub"))) {
            try {
                // read base64 encoded public key from file, process it and generate PublicKey
                final byte[] keyBytes = Files.readAllBytes(cert.toPath());
                final String keyString = new String(keyBytes);
                if (!keyString.startsWith(PUBLIC_KEY_PREFIX)) {
                    if (keyString.startsWith(SSH_RSA_PREFIX)) {
                        final String command = "ssh-keygen -f " + cert.getName() + " -e -m pkcs8 > "+ cert.getName();
                        getLogger().warn("Key \""+ cert.getAbsolutePath() +"\" is not in a usable format. Use the command '"
                            + command + "' to convert it to a PEM format.");
                    } else {
                        getLogger().warn("Key \""+ cert.getAbsolutePath() +"\" is not in a usable format. Public keys must "
                            + "be a x509 encoded PEM format.");
                    }
                    continue;
                }

                final String formattedKeyString = keyString.replaceAll(PUBLIC_KEY_PREFIX, "").replaceAll(PUBLIC_KEY_SUFFIX, "");
                final byte[] decoded = Base64.decode(formattedKeyString.getBytes());
                final X509EncodedKeySpec spec = new X509EncodedKeySpec(decoded);
                final PublicKey publicKey = kf.generatePublic(spec);

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
                    Claims body = jws.getBody();
                    for (Entry<String, Object> entry : body.entrySet()) {
                        flowFile = session.putAttribute(flowFile, JWT_PREFIX_ATTRIBUTE+entry.getKey(),entry.getValue().toString());
                    }
                    session.transfer(flowFile, SUCCESS_REL);
                }
                return;
            } catch (JwtException ex) {
                // expect these for certs that do not unsign the jwt
            } catch (Exception ex) {
                getLogger().warn("Error during unsign process: " + ex);
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