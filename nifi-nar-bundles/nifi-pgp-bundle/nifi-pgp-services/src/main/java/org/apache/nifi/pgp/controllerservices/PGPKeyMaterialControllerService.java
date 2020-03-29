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
package org.apache.nifi.pgp.controllerservices;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.pgp.PGPKeyMaterialService;
import org.apache.nifi.security.pgp.PGPOperator;
import org.apache.nifi.security.pgp.StandardPGPOperator;
import org.bouncycastle.openpgp.PGPException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;


@CapabilityDescription("Defines cryptographic key material and cryptographic operations for PGP processors.")
@Tags({"pgp", "gpg", "encryption", "credentials", "provider"})
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.ACCESS_ENCRYPTION_KEY,
                        explanation = "Provides operator the ability to access and use encryption keys assuming all permissions that NiFi has.")
        }
)

public class PGPKeyMaterialControllerService extends AbstractControllerService implements PGPKeyMaterialService {
    private static final String CONTROLLER_NAME = "PGP Key Material Controller Service";
    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            StandardPGPOperator.PUBLIC_KEYRING_FILE,
            StandardPGPOperator.PUBLIC_KEYRING_TEXT,
            StandardPGPOperator.PUBLIC_KEY_USER_ID,
            StandardPGPOperator.SECRET_KEYRING_FILE,
            StandardPGPOperator.SECRET_KEYRING_TEXT,
            StandardPGPOperator.SECRET_KEY_USER_ID,
            StandardPGPOperator.PRIVATE_KEY_PASSPHRASE,
            StandardPGPOperator.PBE_PASSPHRASE
    ));

    private PGPOperator operator = new StandardPGPOperator(); // One operator per controller


    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        super.init(config);
        operator = new StandardPGPOperator();
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }


    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final StandardPGPOperator operator = ((StandardPGPOperator) this.operator);
        final ArrayList<ValidationResult> problems = new ArrayList<>();
        boolean configured = false;

        if (operator.isContextForEncrypt(context)) {
            configured = true;

            if (!operator.isContextForEncryptValid((context)))
                problems.add(new ValidationResult.Builder()
                        .subject(CONTROLLER_NAME)
                        .valid(false)
                        .explanation("Controller requires a valid Public Key or PBE passphrase for Encrypt operations.")
                        .build());
        }

        if (operator.isContextForDecrypt(context)) {
            configured = true;

            if (!operator.isContextForDecryptValid(context))
                problems.add(new ValidationResult.Builder()
                        .subject(CONTROLLER_NAME)
                        .valid(false)
                        .explanation("Controller requires a valid Secret Key or PBE passphrase for Decrypt operations.")
                        .build());
        }

        if (operator.isContextForSign(context)) {
            configured = true;

            if (!operator.isContextForSignValid(context))
                problems.add(new ValidationResult.Builder()
                        .subject(CONTROLLER_NAME)
                        .valid(false)
                        .explanation("Controller requires a valid Secret Key for Sign operations.")
                        .build());
        }

        if (operator.isContextForVerify(context)) {
            configured = true;

            if (!operator.isContextForVerifyValid(context))
                problems.add(new ValidationResult.Builder()
                        .subject(CONTROLLER_NAME)
                        .valid(false)
                        .explanation("Controller requires a valid Public Key for Verify operations.")
                        .build());
        }

        return configured ? problems : Collections.singletonList(new ValidationResult.Builder()
                .subject(CONTROLLER_NAME)
                .valid(false)
                .explanation("Controller not configured for any operation.")
                .build());
    }


    /**
     * Encrypts a {@link FlowFile}.
     *
     * @param flow flow to encrypt
     * @param context properties for encryption options
     * @param session processing session
     * @return encrypted flow
     */
    @Override
    public FlowFile encrypt(FlowFile flow, PropertyContext context, ProcessSession session) {
        return session.write(flow, (in, out) -> {
            try {
                operator.encrypt(in, out, operator.optionsForEncrypt(getCombinedPropertyContext(context)));
            } catch (final PGPException e) {
                throw new ProcessException(e);
            }
        });
    }


    /**
     * Decrypts a {@link FlowFile}.
     *
     * @param flow flow to decrypt
     * @param context properties for the decryption options
     * @param session processing session
     * @return decrypted flow
     */
    @Override
    public FlowFile decrypt(FlowFile flow, PropertyContext context, ProcessSession session) {
        return session.write(flow, (in, out) -> {
            operator.decrypt(in, out, operator.optionsForDecrypt(getCombinedPropertyContext(context)));
        });
    }


    /**
     * Signs a {@link FlowFile}.
     *
     * @param flow flow to sign
     * @param context properties for signing options
     * @param session processing session
     * @return signature output stream
     */
    @Override
    public byte[] sign(FlowFile flow, PropertyContext context, ProcessSession session) throws ProcessException {
        final ByteArrayOutputStream signature = new ByteArrayOutputStream();
        try {
            operator.sign(session.read(flow), signature, operator.optionsForSign(getCombinedPropertyContext(context)));
        } catch (final IOException | PGPException e) {
            throw new ProcessException(e);
        }
        return signature.toByteArray();
    }


    /**
     * Verify a {@link FlowFile}.
     *
     * @param flow flow to verify
     * @param context properties for verification session
     * @param session processing session
     * @return true if the flow can be verified with signature, false if cannot be verified
     */
    @Override
    public boolean verify(FlowFile flow, PropertyContext context, ProcessSession session) throws ProcessException {
        try {
            return operator.verify(session.read(flow), operator.getSignature(context, flow), operator.optionsForVerify(getCombinedPropertyContext(context)));
        } catch (final IOException | PGPException e) {
            throw new ProcessException(e);
        }
    }


    /**
     * Provides a new property context based on the one given merged with the configuration context of this controller instance.
     *
     * @param context properties for PGP operations, typically from a processor
     * @return combined properties
     */
    private PropertyContext getCombinedPropertyContext(PropertyContext context) {
        return new PropertyContext() {
            @Override
            public PropertyValue getProperty(PropertyDescriptor descriptor) {
                if (context.getProperty(descriptor).isSet())
                    return context.getProperty(descriptor);
                return getConfigurationContext().getProperty(descriptor);
            }

            @Override
            public Map<String, String> getAllProperties() {
                final Map<String, String> properties = context.getAllProperties();
                properties.putAll(getConfigurationContext().getAllProperties());
                return properties;
            }
        };
    }
}
