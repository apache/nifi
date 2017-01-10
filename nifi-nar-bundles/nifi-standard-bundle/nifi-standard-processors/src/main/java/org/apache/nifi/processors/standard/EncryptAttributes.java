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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.crypto.EncryptProcessorUtils;
import org.apache.nifi.processors.standard.util.crypto.EncryptProcessorUtils.Encryptor;
import org.apache.nifi.processors.standard.util.crypto.KeyedEncryptor;
import org.apache.nifi.processors.standard.util.crypto.OpenPGPKeyBasedEncryptor;
import org.apache.nifi.processors.standard.util.crypto.OpenPGPPasswordBasedEncryptor;
import org.apache.nifi.processors.standard.util.crypto.PasswordBasedEncryptor;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Provides functionality of encrypting attributes with various algorithms.
 * Note. It'll not modify filename or uuid as they are sensitive and are
 * internally used by either Algorithm itself or FlowFile repo.
 */
@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"encryption", "decryption", "password", "JCE", "OpenPGP", "PGP", "GPG", "regex",
        "regexp", "Attribute Expression Language"})
@CapabilityDescription("Encrypts or Decrypts a FlowFile attributes using either symmetric encryption with a password " +
        "and randomly generated salt, or asymmetric encryption using a public and secret key. Different options are " +
        "available to provide list of attributes. Default options are: 'all-attributes'/'core-attributes/" +
        "'all-except-core-attributes'. You can also add custom properties containing expression language condition. " +
        "These conditions will be evaluated and only those attributes will be considered for which the condition " +
        "is \'true\'. You can also provide RegEx to select a group of attributes. RegEx and Expression Language conditions" +
        "can be combined for advanced filtering of attribute list")
@DynamicProperty(name = "Attribute Name", value = "Attribute Expression Language", description = "Evaluates expression language " +
        "as boolean expression, if attribute exist and boolean condition evaluates to true, then it'll be considered " +
        "for encryption/decryption")
public class EncryptAttributes extends AbstractProcessor {

    public static final String ENCRYPT_MODE = "Encrypt";
    public static final String DECRYPT_MODE = "Decrypt";

    public static final String WEAK_CRYPTO_ALLOWED_NAME = "allowed";
    public static final String WEAK_CRYPTO_NOT_ALLOWED_NAME = "not-allowed";
    public static final String ALL_ATTR = "All Attributes";
    public static final String CORE_ATTR = "Core Attributes";
    public static final String ALL_EXCEPT_CORE_ATTR = "All Except Core Attributes";
    public static final String CUSTOM_ATTR = "Custom Attributes";

    private static final AllowableValue ALL_ATTR_ALLOWABLE_VALUE = new AllowableValue(ALL_ATTR, ALL_ATTR,
            "All attributes will be considered for encryption/decryption. Note: \'uuid\' attribute will be ignored. " +
                    "If using PGP algo for encryption/decryption then \'filename\' will be ignored");
    private static final AllowableValue CORE_ATTR_ALLOWABLE_VALUE = new AllowableValue(CORE_ATTR, CORE_ATTR,
            "Core attributes will be considered for encryption/decryption. Note: \'uuid\' attribute will be ignored.");
    private static final AllowableValue ALL_EXCEPT_CORE_ATTR_ALLOWABLE_VALUE = new AllowableValue(ALL_EXCEPT_CORE_ATTR,
            CORE_ATTR, "All attributes except core attributes will be considered for encryption/decryption.");
    private static final AllowableValue CUSTOM_ATTR_ALLOWABLE_VALUE = new AllowableValue(CUSTOM_ATTR, CUSTOM_ATTR,
            "Custom filters can applied on attribute list via providing RegEx in provied property or can add " +
                    "Custom Expression Language conditions which will consider only those attributes to which it evaluates " +
                    "to true. Note: \'uuid\' ignored and if using PGP encryption/decryption the \'filename\' will also be ignored");

    public static final PropertyDescriptor ATTRS_TO_ENCRYPT = new PropertyDescriptor.Builder()
            .name("attributes-to-encrypt")
            .displayName("Attributes to Encrypt")
            .description("Choose the attributes you would like to encrypt. You can also dynamic properties " +
                    "with Expression Language condition, if matches then it'll be encrypted otherwise ignored.")
            .required(true)
            .allowableValues(ALL_EXCEPT_CORE_ATTR_ALLOWABLE_VALUE, ALL_ATTR_ALLOWABLE_VALUE, CORE_ATTR_ALLOWABLE_VALUE,
                    CUSTOM_ATTR_ALLOWABLE_VALUE)
            .defaultValue(ALL_ATTR_ALLOWABLE_VALUE.getValue())
            .build();
    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("mode")
            .displayName("Mode")
            .description("Specifies whether the content should be encrypted or decrypted")
            .required(true)
            .allowableValues(ENCRYPT_MODE, DECRYPT_MODE)
            .defaultValue(ENCRYPT_MODE)
            .build();
    public static final PropertyDescriptor KEY_DERIVATION_FUNCTION = new PropertyDescriptor.Builder()
            .name(EncryptProcessorUtils.KEY_DERIVATION_FUNCTION)
            .displayName("Key Derivation Function")
            .description("Specifies the key derivation function to generate the key from the password (and salt)")
            .required(true)
            .allowableValues(EncryptProcessorUtils.buildKeyDerivationFunctionAllowableValues())
            .defaultValue(KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY.name())
            .build();
    public static final PropertyDescriptor ENCRYPTION_ALGORITHM = new PropertyDescriptor.Builder()
            .name(EncryptProcessorUtils.ENCRYPTION_ALGORITHM)
            .displayName("Encryption Algorithm")
            .description("The Encryption Algorithm to use")
            .required(true)
            .allowableValues(EncryptProcessorUtils.buildEncryptionMethodAllowableValues())
            .defaultValue(EncryptionMethod.MD5_128AES.name())
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name(EncryptProcessorUtils.PASSWORD)
            .displayName("Password")
            .description("The Password to use for encrypting or decrypting the data")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor PUBLIC_KEYRING = new PropertyDescriptor.Builder()
            .name(EncryptProcessorUtils.PRIVATE_KEYRING)
            .displayName("Public Keyring File")
            .description("In a PGP encrypt mode, this keyring contains the public key of the recipient")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PUBLIC_KEY_USERID = new PropertyDescriptor.Builder()
            .name(EncryptProcessorUtils.PUBLIC_KEY_USERID)
            .displayName("Public Key User Id")
            .description("In a PGP encrypt mode, this user id of the recipient")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PRIVATE_KEYRING = new PropertyDescriptor.Builder()
            .name(EncryptProcessorUtils.PRIVATE_KEYRING)
            .displayName("Private Keyring File")
            .description("In a PGP decrypt mode, this keyring contains the private key of the recipient")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PRIVATE_KEYRING_PASSPHRASE = new PropertyDescriptor.Builder()
            .name(EncryptProcessorUtils.PRIVATE_KEYRING_PASSPHRASE)
            .displayName("Private Keyring Passphrase")
            .description("In a PGP decrypt mode, this is the private keyring passphrase")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor RAW_KEY_HEX = new PropertyDescriptor.Builder()
            .name(EncryptProcessorUtils.RAW_KEY_HEX)
            .displayName("Raw Key (hexadecimal)")
            .description("In keyed encryption, this is the raw key, encoded in hexadecimal")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor ALLOW_WEAK_CRYPTO = new PropertyDescriptor.Builder()
            .name("allow-weak-crypto")
            .displayName("Allow insecure cryptographic modes")
            .description("Overrides the default behavior to prevent unsafe combinations of encryption algorithms and short passwords on JVMs with limited strength cryptographic jurisdiction policies")
            .required(true)
            .allowableValues(EncryptProcessorUtils.buildWeakCryptoAllowableValues())
            .defaultValue(EncryptProcessorUtils.buildDefaultWeakCryptoAllowableValue().getValue())
            .build();
    public static final PropertyDescriptor ATTR_SELECT_REG_EX = new PropertyDescriptor.Builder()
            .name("attribute-select-regex")
            .displayName("Attributes Selection RegEx")
            .description("If " + CUSTOM_ATTR_ALLOWABLE_VALUE.getDisplayName() + " is selcted then provied a RegEx to select " +
                    "attributes matching a specific pattern. Only those attributes will be encrypted/decrypted " +
                    "who matches against given regex pattern")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(".*")
            .required(false)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Any FlowFile that is successfully encrypted or decrypted will be routed to success").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Any FlowFile that cannot be encrypted or decrypted will be routed to failure").build();
    private List<PropertyDescriptor> properties;

    private Set<Relationship> relationships;

    private volatile Map<String, PropertyValue> propMap = new HashMap<>();

    static {
        // add BouncyCastle encryption providers
        Security.addProvider(new BouncyCastleProvider());
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATTRS_TO_ENCRYPT);
        properties.add(MODE);
        properties.add(KEY_DERIVATION_FUNCTION);
        properties.add(ENCRYPTION_ALGORITHM);
        properties.add(ALLOW_WEAK_CRYPTO);
        properties.add(PASSWORD);
        properties.add(RAW_KEY_HEX);
        properties.add(PUBLIC_KEYRING);
        properties.add(PUBLIC_KEY_USERID);
        properties.add(PRIVATE_KEYRING);
        properties.add(PRIVATE_KEYRING_PASSPHRASE);
        properties.add(ATTR_SELECT_REG_EX);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));
        final String methodValue = context.getProperty(ENCRYPTION_ALGORITHM).getValue();
        final EncryptionMethod encryptionMethod = EncryptionMethod.valueOf(methodValue);
        final String algorithm = encryptionMethod.getAlgorithm();
        final String password = context.getProperty(PASSWORD).getValue();
        final KeyDerivationFunction kdf = KeyDerivationFunction.valueOf(context.getProperty(KEY_DERIVATION_FUNCTION).getValue());
        final String keyHex = context.getProperty(RAW_KEY_HEX).getValue();
        if (EncryptProcessorUtils.isPGPAlgorithm(algorithm)) {
            final boolean encrypt = context.getProperty(MODE).getValue().equalsIgnoreCase(ENCRYPT_MODE);
            final String publicKeyring = context.getProperty(PUBLIC_KEYRING).getValue();
            final String publicUserId = context.getProperty(PUBLIC_KEY_USERID).getValue();
            final String privateKeyring = context.getProperty(PRIVATE_KEYRING).getValue();
            final String privateKeyringPassphrase = context.getProperty(PRIVATE_KEYRING_PASSPHRASE).getValue();
            validationResults.addAll(EncryptProcessorUtils.validatePGP(encryptionMethod, password, encrypt, publicKeyring, publicUserId, privateKeyring, privateKeyringPassphrase));
        } else { // Not PGP
            if (encryptionMethod.isKeyedCipher()) { // Raw key
                validationResults.addAll(EncryptProcessorUtils.validateKeyed(encryptionMethod, kdf, keyHex));
            } else { // PBE
                boolean allowWeakCrypto = context.getProperty(ALLOW_WEAK_CRYPTO).getValue().equalsIgnoreCase(WEAK_CRYPTO_ALLOWED_NAME);
                validationResults.addAll(EncryptProcessorUtils.validatePBE(encryptionMethod, kdf, password, allowWeakCrypto));
            }
        }
        return validationResults;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.BOOLEAN, false))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .dynamic(true)
                .required(false)
                .expressionLanguageSupported(true)
                .build();
    }

    /**
     * Performs decryption with given input string and encryptor.
     * The input must be of Base64 encoded string.
     *
     * @param str       Base64 encoded encrypted String
     * @param encryptor Encryptor which will be used for decryption
     * @return decrypted string of charset US-ASCII
     * @throws Exception exception if couldn't process streams converted from strings
     */
    private String performDecryption(String str, Encryptor encryptor) throws Exception {
        //Initialize string and streams
        byte[] encryptedBytes = str.getBytes(StandardCharsets.US_ASCII);
        byte[] decodedBytes = Base64.decodeBase64(encryptedBytes);
        String decryptedStr;

        try (InputStream in = new ByteArrayInputStream(decodedBytes);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            encryptor.getDecryptionCallback().process(in, out);
            decryptedStr = new String(out.toByteArray(), StandardCharsets.US_ASCII);
        } catch (IOException e) {
            throw new ProcessException(e);
        }

        return decryptedStr;
    }

    /**
     * Performs encryption with given input string. The final encrypted string is
     * encoded to Base64 to prevent data loss
     *
     * @param str       String to be encrypted
     * @param encryptor Encryptor which will be used for encryption
     * @return Base64 encode string after performing encryption
     * @throws Exception exception if couldn't process streams converted from strings
     */
    private String performEncryption(String str, Encryptor encryptor) throws Exception {
        String encodedEncryptedStr;

        try (InputStream in = new ByteArrayInputStream(str.getBytes(StandardCharsets.US_ASCII));
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            encryptor.getEncryptionCallback().process(in, out);
            byte[] encryptedData = out.toByteArray();
            encodedEncryptedStr = Base64.encodeBase64String(encryptedData);
        } catch (IOException e) {
            throw new ProcessException(e);
        }
        return encodedEncryptedStr;
    }

    private Set<String> getAttrToEncrypt(FlowFile flowFile, PropertyValue attrsToEncryptProp,
                                         PropertyValue attrSelectRegEx, String algorithm, boolean encrypt) {
        String attrsToEncryptPropVal = attrsToEncryptProp.getValue();
        String regex = attrSelectRegEx.getValue();
        Set<String> flowFileAttrs = flowFile.getAttributes().keySet();
        Set<String> attrsToEncrypt;

        if (attrsToEncryptPropVal.equals(CORE_ATTR)) {
            attrsToEncrypt = new HashSet<>();
        } else {
            attrsToEncrypt = new HashSet<>(flowFileAttrs);
        }

        if (attrsToEncryptPropVal.equals(ALL_EXCEPT_CORE_ATTR)
                || attrsToEncryptPropVal.equals(CORE_ATTR)) {
            //traverse core attributes and add/remove as per the prop value.
            for (CoreAttributes attr : CoreAttributes.values()) {
                if (flowFileAttrs.contains(attr.key()) && attrsToEncryptPropVal.equals(ALL_EXCEPT_CORE_ATTR)) {
                    attrsToEncrypt.remove(attr.key());
                } else if (flowFileAttrs.contains(attr.key()) && attrsToEncryptPropVal.equals(CORE_ATTR)) {
                    attrsToEncrypt.add(attr.key());
                }
            }
        }

        if (attrsToEncryptPropVal.equals(CUSTOM_ATTR)) {

            //get list of all the attributes matching regex
            if (regex != null && !regex.equals(".*")) {
                attrsToEncrypt.clear();
                Pattern pattern = Pattern.compile(regex);
                for (String str : flowFileAttrs) {
                    if (pattern.matcher(str).matches()) {
                        attrsToEncrypt.add(str);
                    }
                }
            }

            //check if property-key is present in attrsToEncrypt and if expression-lang condition
            //return true then encrypt/decrypt it.
            if (!propMap.isEmpty()) {
                HashSet<String> attrsToEncryptClone = new HashSet<>(attrsToEncrypt);
                for(String attr: attrsToEncryptClone) {
                    if (propMap.containsKey(attr)) {
                        boolean matches = propMap.get(attr).evaluateAttributeExpressions(flowFile).asBoolean();
                        if (!matches){
                            attrsToEncrypt.remove(attr);
                            getLogger().warn("{} expression-language expression evaluates to false",
                                    new Object[]{propMap.get(attr).getValue()});
                        }
                    } else {
                        attrsToEncrypt.remove(attr);
                    }
                }

            }
        }

        attrsToEncrypt.remove(CoreAttributes.UUID.key());
        if (EncryptProcessorUtils.isPGPAlgorithm(algorithm)) {
            attrsToEncrypt.remove(CoreAttributes.FILENAME.key());
            getLogger().info("Removing filename from {}cryption because of {} algorithm",
                    new Object[]{(encrypt)?"en":"de", algorithm});
        }
        return attrsToEncrypt;
    }

    private Map<String, String> buildNewAttributes(FlowFile flowFile, PropertyValue attrList,
                                                   PropertyValue attrSelectRegex, String algorithm,
                                                   Encryptor encryptor, boolean encrypt) throws Exception {

        Map<String, String> oldAttrs = flowFile.getAttributes();
        Map<String, String> newAttrs = new HashMap<>();
        Set<String> attrToEncrypt = getAttrToEncrypt(flowFile, attrList, attrSelectRegex, algorithm, encrypt);

        for (String attr : attrToEncrypt) {
            String attrVal = oldAttrs.get(attr);
            String encryptedVal = (encrypt) ? performEncryption(attrVal, encryptor) : performDecryption(attrVal, encryptor);
            newAttrs.put(attr, encryptedVal);
            getLogger().debug("{}crypted {} from '{}' to '{}'",
                    new Object[]{(encrypt)?"en":"de", attr, attrVal, encryptedVal});
        }

        return newAttrs;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        propMap = new HashMap<>();
        for (PropertyDescriptor propDescriptor : context.getProperties().keySet()) {
            if (propDescriptor.isDynamic()) {
                propMap.put(propDescriptor.getName(), context.getProperty(propDescriptor));
                getLogger().info("Adding dynamic property: {}", new Object[]{propDescriptor});
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final PropertyValue attrList = context.getProperty(ATTRS_TO_ENCRYPT);
        final PropertyValue attrSelectRegex = context.getProperty(ATTR_SELECT_REG_EX);
        final String method = context.getProperty(ENCRYPTION_ALGORITHM).getValue();
        final EncryptionMethod encryptionMethod = EncryptionMethod.valueOf(method);
        final String providerName = encryptionMethod.getProvider();
        final String algorithm = encryptionMethod.getAlgorithm();
        final String password = context.getProperty(PASSWORD).getValue();
        final KeyDerivationFunction kdf = KeyDerivationFunction.valueOf(context.getProperty(KEY_DERIVATION_FUNCTION).getValue());
        final boolean encrypt = context.getProperty(MODE).getValue().equalsIgnoreCase(ENCRYPT_MODE);

        Encryptor encryptor;
        Map<String, String> newAtrList;

        try {
            if (EncryptProcessorUtils.isPGPAlgorithm(algorithm)) {
                final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
                final String publicKeyring = context.getProperty(PUBLIC_KEYRING).getValue();
                final String privateKeyring = context.getProperty(PRIVATE_KEYRING).getValue();
                if (encrypt && publicKeyring != null) {
                    final String publicUserId = context.getProperty(PUBLIC_KEY_USERID).getValue();
                    encryptor = new OpenPGPKeyBasedEncryptor(algorithm, providerName, publicKeyring, publicUserId, null, filename);
                } else if (!encrypt && privateKeyring != null) {
                    final char[] keyringPassphrase = context.getProperty(PRIVATE_KEYRING_PASSPHRASE).getValue().toCharArray();
                    encryptor = new OpenPGPKeyBasedEncryptor(algorithm, providerName, privateKeyring, null, keyringPassphrase,
                            filename);
                } else {
                    final char[] passphrase = Normalizer.normalize(password, Normalizer.Form.NFC).toCharArray();
                    encryptor = new OpenPGPPasswordBasedEncryptor(algorithm, providerName, passphrase, filename);
                }
            } else if (kdf.equals(KeyDerivationFunction.NONE)) { // Raw key
                final String keyHex = context.getProperty(RAW_KEY_HEX).getValue();
                encryptor = new KeyedEncryptor(encryptionMethod, Hex.decodeHex(keyHex.toCharArray()));
            } else { // PBE
                final char[] passphrase = Normalizer.normalize(password, Normalizer.Form.NFC).toCharArray();
                encryptor = new PasswordBasedEncryptor(encryptionMethod, passphrase, kdf);
            }

            newAtrList = buildNewAttributes(flowFile, attrList, attrSelectRegex, algorithm, encryptor, encrypt);
            FlowFile newFlowFile = session.putAllAttributes(flowFile, newAtrList);
            session.transfer(newFlowFile, REL_SUCCESS);

        } catch (final Exception e) {
            logger.error(e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }

    }
}
