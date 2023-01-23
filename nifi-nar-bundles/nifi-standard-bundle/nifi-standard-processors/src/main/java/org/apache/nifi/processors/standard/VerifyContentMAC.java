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

package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Encryption", "Signing", "MAC", "HMAC"})
@CapabilityDescription("Calculates MAC using the provided Secret Key and compares it with the provided MAC parameter")
@EventDriven
@WritesAttributes({
    @WritesAttribute(attribute = VerifyContentMAC.MAC_CALCULATED_ATTRIBUTE, description = "The calculated MAC value"),
    @WritesAttribute(attribute = VerifyContentMAC.MAC_ALGORITHM_ATTRIBUTE, description = "The MAC algorithm")})
public class VerifyContentMAC extends AbstractProcessor {

    protected static final String HMAC_SHA256 = "HmacSHA256";
    protected static final String HMAC_SHA512 = "HmacSHA512";

    protected static final PropertyDescriptor MAC_ALGORITHM = new PropertyDescriptor.Builder()
        .name("MAC_ALGORITHM")
        .displayName("MAC Algorithm")
        .description("Cryptographic Hash Function")
        .allowableValues(HMAC_SHA256, HMAC_SHA512)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    protected static final PropertyDescriptor SECRET_KEY = new PropertyDescriptor.Builder()
        .name("SECRET_KEY")
        .displayName("Secret key")
        .description("Cryptographic key to calculate the hash")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(true)
        .build();

    protected static final PropertyDescriptor MAC = new PropertyDescriptor.Builder()
        .name("MAC")
        .displayName("Message Authentication Code")
        .description("The MAC to compare with the calculated value")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    protected static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Signature Verification Failed")
        .build();

    protected static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Signature Verification Succeeded")
        .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(MAC_ALGORITHM, SECRET_KEY, MAC));
    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(SUCCESS, FAILURE)));;
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final int BUFFER_SIZE = 512000;
    private static final String HEX_FORMAT = "%02x";
    protected static final String MAC_CALCULATED_ATTRIBUTE = "mac.calculated";
    protected static final String MAC_ALGORITHM_ATTRIBUTE = "mac.algorithm";

    private SecretKeySpec secretKeySpec;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void setUp(ProcessContext context)  {
        secretKeySpec = new SecretKeySpec(context.getProperty(SECRET_KEY).getValue().getBytes(CHARSET), HMAC_SHA256);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        try {
            String incomingMac = context.getProperty(MAC).evaluateAttributeExpressions(flowFile).getValue();
            String algorithm = context.getProperty(MAC_ALGORITHM).getValue();
            String calculatedMac = getCalculatedMac(session, flowFile, algorithm);

            flowFile = enrichFlowFileAttributes(session, flowFile, algorithm, calculatedMac);

            if (!incomingMac.equals(calculatedMac)) {
                session.transfer(flowFile, FAILURE);
            } else {
                session.transfer(flowFile, SUCCESS);
            }
        } catch (IOException e) {
            getLogger().error("Request Processing failed: {}", flowFile, e);
            session.penalize(flowFile);
            session.transfer(flowFile, FAILURE);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            getLogger().error("Encountered an error during HMAC generation: ", e);
            context.yield();
        }
    }

    private FlowFile enrichFlowFileAttributes(ProcessSession session, FlowFile flowFile, String algorithm, String calculatedMac) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(MAC_ALGORITHM_ATTRIBUTE, algorithm);
        attributes.put(MAC_CALCULATED_ATTRIBUTE, calculatedMac);
        flowFile = session.putAllAttributes(flowFile, attributes);
        return flowFile;
    }

    private String getCalculatedMac(ProcessSession session, FlowFile flowFile, String algorithm) throws NoSuchAlgorithmException, InvalidKeyException, IOException {
        Mac mac = Mac.getInstance(algorithm);
        mac.init(secretKeySpec);

        byte[] contents = new byte[BUFFER_SIZE];
        int readSize;

        try (InputStream is = session.read(flowFile)){
            while ((readSize = is.read(contents)) != -1) {
                mac.update(contents, 0, readSize);
            }
        }

        return bytesToHex(mac.doFinal());
    }

    private String bytesToHex(byte[] in) {
        StringBuilder builder = new StringBuilder();
        for (byte b : in) {
            builder.append(String.format(HEX_FORMAT, b));
        }
        return builder.toString();
    }
}
