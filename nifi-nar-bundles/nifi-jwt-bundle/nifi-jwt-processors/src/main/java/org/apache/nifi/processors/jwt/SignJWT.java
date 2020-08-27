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
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.userauth.keyprovider.KeyProvider;
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
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivateKey;
import java.security.Security;
import java.util.*;

@InputRequirement(Requirement.INPUT_REQUIRED)
@SideEffectFree
@Tags({ "jwt", "json", "flowfile", "sign" })
@CapabilityDescription("JWT Signs a JSON file with a specified private key. The JSON must be key value pairs.")
@WritesAttributes({
    @WritesAttribute(attribute = "failure.reason", description = "If the flow file is routed to the failure relationship "
            + "the attribute will contain the error message resulting from the validation failure.")
})
@SeeAlso({ UnsignJWT.class })
public class SignJWT extends AbstractProcessor {

    public static final PropertyDescriptor PRIVATE_KEY_PATH = new PropertyDescriptor.Builder()
            .name("PRIVATE_KEY_PATH").displayName("Private Key").description("Path to private key to sign the JWT. The key must be readable by the NiFi user")
            .required(true).addValidator(CustomValidators.PRIVATE_KEY_VALIDATOR).build();

    public static final Relationship SUCCESS_REL = new Relationship.Builder().name("success")
            .description("A FlowFile is routed to this relationship after its JWT has been validated and unsigned")
            .build();

    public static final Relationship FAILURE_REL = new Relationship.Builder().name("failure").description(
            "A FlowFile is routed to this relationship if the JWT cannot be validated or unsigned for any reason")
            .build();

    public static final String FAILURE_REASON_ATTR = "failure.reason";
    public static final String INVALID_JSON = "Invalid JSON: ";
    public static final String ERROR_PREFIX = "Error signing JWT: ";

    private PrivateKey privateKey;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PRIVATE_KEY_PATH);
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final File key = new File(context.getProperty(PRIVATE_KEY_PATH).getValue());
        KeyProvider keyProvider;
        try (final SSHClient sshClient = new SSHClient()) {
            keyProvider = sshClient.loadKeys(key.getAbsolutePath());
            privateKey = keyProvider.getPrivate();
        } catch (IOException e) {
            privateKey = null;
        }
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
        } catch (JsonProcessingException ex) {
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTR, INVALID_JSON  + ex.getLocalizedMessage());
            session.getProvenanceReporter().route(flowFile, FAILURE_REL);
            session.transfer(flowFile, FAILURE_REL);
        } catch (JwtException | IOException ex) {
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTR, ERROR_PREFIX + ex.getLocalizedMessage());
            session.getProvenanceReporter().route(flowFile, FAILURE_REL);
            session.transfer(flowFile, FAILURE_REL);
        }
    }
}