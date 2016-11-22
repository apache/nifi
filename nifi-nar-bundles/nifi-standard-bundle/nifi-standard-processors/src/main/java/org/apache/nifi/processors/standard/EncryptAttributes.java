package org.apache.nifi.processors.standard;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.crypto.*;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.text.Normalizer;
import java.util.*;

/**
 * Provides functionality of encrypting attributes with various algorithms.
 * Note. It'll not modify filename or uuid as they are sensitive and are
 * internally used by either Algorithm itself or FlowFile repo.
 */
@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"encryption", "decryption", "password", "JCE", "OpenPGP", "PGP", "GPG"})
@CapabilityDescription("Encrypts or Decrypts a FlowFile attributes using either symmetric encryption with a password " +
        "and randomly generated salt, or asymmetric encryption using a public and secret key.")
public class EncryptAttributes extends AbstractProcessor {

    public static final String ENCRYPT_MODE = "Encrypt";
    public static final String DECRYPT_MODE = "Decrypt";

    private static final String WEAK_CRYPTO_ALLOWED_NAME = "allowed";
    private static final String WEAK_CRYPTO_NOT_ALLOWED_NAME = "not-allowed";

    public static final PropertyDescriptor ATTRIBUTES_TO_ENCRYPT = new PropertyDescriptor.Builder()
            .name("Attributes to encrypte")
            .description("Comma separated list of attributes to encrypt, if empty then it'll encrypt all the " +
                    "attributes")
            .required(false)
            .build();
    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("Mode")
            .description("Specifies whether the content should be encrypted or decrypted")
            .required(true)
            .allowableValues(ENCRYPT_MODE, DECRYPT_MODE)
            .defaultValue(ENCRYPT_MODE)
            .build();
    public static final PropertyDescriptor KEY_DERIVATION_FUNCTION = new PropertyDescriptor.Builder()
            .name("key-derivation-function")
            .displayName("Key Derivation Function")
            .description("Specifies the key derivation function to generate the key from the password (and salt)")
            .required(true)
            .allowableValues(buildKeyDerivationFunctionAllowableValues())
            .defaultValue(KeyDerivationFunction.BCRYPT.name())
            .build();
    public static final PropertyDescriptor ENCRYPTION_ALGORITHM = new PropertyDescriptor.Builder()
            .name("Encryption Algorithm")
            .description("The Encryption Algorithm to use")
            .required(true)
            .allowableValues(buildEncryptionMethodAllowableValues())
            .defaultValue(EncryptionMethod.MD5_128AES.name())
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The Password to use for encrypting or decrypting the data")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor PUBLIC_KEYRING = new PropertyDescriptor.Builder()
            .name("public-keyring-file")
            .displayName("Public Keyring File")
            .description("In a PGP encrypt mode, this keyring contains the public key of the recipient")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PUBLIC_KEY_USERID = new PropertyDescriptor.Builder()
            .name("public-key-user-id")
            .displayName("Public Key User Id")
            .description("In a PGP encrypt mode, this user id of the recipient")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PRIVATE_KEYRING = new PropertyDescriptor.Builder()
            .name("private-keyring-file")
            .displayName("Private Keyring File")
            .description("In a PGP decrypt mode, this keyring contains the private key of the recipient")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PRIVATE_KEYRING_PASSPHRASE = new PropertyDescriptor.Builder()
            .name("private-keyring-passphrase")
            .displayName("Private Keyring Passphrase")
            .description("In a PGP decrypt mode, this is the private keyring passphrase")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor RAW_KEY_HEX = new PropertyDescriptor.Builder()
            .name("raw-key-hex")
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
            .allowableValues(buildWeakCryptoAllowableValues())
            .defaultValue(buildDefaultWeakCryptoAllowableValue().getValue())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Any FlowFile that is successfully encrypted or decrypted will be routed to success").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Any FlowFile that cannot be encrypted or decrypted will be routed to failure").build();
    private List<PropertyDescriptor> properties;

    private Set<Relationship> relationships;

    static {
        // add BouncyCastle encryption providers
        Security.addProvider(new BouncyCastleProvider());
    }

    private static AllowableValue[] buildKeyDerivationFunctionAllowableValues() {
        final KeyDerivationFunction[] keyDerivationFunctions = KeyDerivationFunction.values();
        List<AllowableValue> allowableValues = new ArrayList<>(keyDerivationFunctions.length);
        for (KeyDerivationFunction kdf : keyDerivationFunctions) {
            allowableValues.add(new AllowableValue(kdf.name(), kdf.getName(), kdf.getDescription()));
        }

        return allowableValues.toArray(new AllowableValue[0]);
    }

    private static AllowableValue[] buildEncryptionMethodAllowableValues() {
        final EncryptionMethod[] encryptionMethods = EncryptionMethod.values();
        List<AllowableValue> allowableValues = new ArrayList<>(encryptionMethods.length);
        for (EncryptionMethod em : encryptionMethods) {
            allowableValues.add(new AllowableValue(em.name(), em.name(), em.toString()));
        }

        return allowableValues.toArray(new AllowableValue[0]);
    }

    private static AllowableValue[] buildWeakCryptoAllowableValues() {
        List<AllowableValue> allowableValues = new ArrayList<>();
        allowableValues.add(new AllowableValue(WEAK_CRYPTO_ALLOWED_NAME, "Allowed", "Operation will not be blocked and no alerts will be presented " +
                "when unsafe combinations of encryption algorithms and passwords are provided"));
        allowableValues.add(buildDefaultWeakCryptoAllowableValue());
        return allowableValues.toArray(new AllowableValue[0]);
    }

    private static AllowableValue buildDefaultWeakCryptoAllowableValue() {
        return new AllowableValue(WEAK_CRYPTO_NOT_ALLOWED_NAME, "Not Allowed", "When set, operation will be blocked and alerts will be presented to the user " +
                "if unsafe combinations of encryption algorithms and passwords are provided on a JVM with limited strength crypto. To fix this, see the Admin Guide.");
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATTRIBUTES_TO_ENCRYPT);
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

    public static boolean isPGPAlgorithm(final String algorithm) {
        return algorithm.startsWith("PGP");
    }

    public static boolean isPGPArmoredAlgorithm(final String algorithm) {
        return isPGPAlgorithm(algorithm) && algorithm.endsWith("ASCII-ARMOR");
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
        if (isPGPAlgorithm(algorithm)) {
            final boolean encrypt = context.getProperty(MODE).getValue().equalsIgnoreCase(ENCRYPT_MODE);
            final String publicKeyring = context.getProperty(PUBLIC_KEYRING).getValue();
            final String publicUserId = context.getProperty(PUBLIC_KEY_USERID).getValue();
            final String privateKeyring = context.getProperty(PRIVATE_KEYRING).getValue();
            final String privateKeyringPassphrase = context.getProperty(PRIVATE_KEYRING_PASSPHRASE).getValue();
            validationResults.addAll(validatePGP(encryptionMethod, password, encrypt, publicKeyring, publicUserId, privateKeyring, privateKeyringPassphrase));
        } else { // Not PGP
            if (encryptionMethod.isKeyedCipher()) { // Raw key
                validationResults.addAll(validateKeyed(encryptionMethod, kdf, keyHex));
            } else { // PBE
                boolean allowWeakCrypto = context.getProperty(ALLOW_WEAK_CRYPTO).getValue().equalsIgnoreCase(WEAK_CRYPTO_ALLOWED_NAME);
                validationResults.addAll(validatePBE(encryptionMethod, kdf, password, allowWeakCrypto));
            }
        }
        return validationResults;
    }

    private List<ValidationResult> validatePGP(EncryptionMethod encryptionMethod, String password, boolean encrypt, String publicKeyring, String publicUserId, String privateKeyring,
                                               String privateKeyringPassphrase) {
        List<ValidationResult> validationResults = new ArrayList<>();

        if (password == null) {
            if (encrypt) {
                // If encrypting without a password, require both public-keyring-file and public-key-user-id
                if (publicKeyring == null || publicUserId == null) {
                    validationResults.add(new ValidationResult.Builder().subject(PUBLIC_KEYRING.getDisplayName())
                            .explanation(encryptionMethod.getAlgorithm() + " encryption without a " + PASSWORD.getDisplayName() + " requires both "
                                    + PUBLIC_KEYRING.getDisplayName() + " and " + PUBLIC_KEY_USERID.getDisplayName())
                            .build());
                } else {
                    // Verify the public keyring contains the user id
                    try {
                        if (OpenPGPKeyBasedEncryptor.getPublicKey(publicUserId, publicKeyring) == null) {
                            validationResults.add(new ValidationResult.Builder().subject(PUBLIC_KEYRING.getDisplayName())
                                    .explanation(PUBLIC_KEYRING.getDisplayName() + " " + publicKeyring
                                            + " does not contain user id " + publicUserId)
                                    .build());
                        }
                    } catch (final Exception e) {
                        validationResults.add(new ValidationResult.Builder().subject(PUBLIC_KEYRING.getDisplayName())
                                .explanation("Invalid " + PUBLIC_KEYRING.getDisplayName() + " " + publicKeyring
                                        + " because " + e.toString())
                                .build());
                    }
                }
            } else { // Decrypt
                // Require both private-keyring-file and private-keyring-passphrase
                if (privateKeyring == null || privateKeyringPassphrase == null) {
                    validationResults.add(new ValidationResult.Builder().subject(PRIVATE_KEYRING.getName())
                            .explanation(encryptionMethod.getAlgorithm() + " decryption without a " + PASSWORD.getDisplayName() + " requires both "
                                    + PRIVATE_KEYRING.getDisplayName() + " and " + PRIVATE_KEYRING_PASSPHRASE.getDisplayName())
                            .build());
                } else {
                    final String providerName = encryptionMethod.getProvider();
                    // Verify the passphrase works on the private keyring
                    try {
                        if (!OpenPGPKeyBasedEncryptor.validateKeyring(providerName, privateKeyring, privateKeyringPassphrase.toCharArray())) {
                            validationResults.add(new ValidationResult.Builder().subject(PRIVATE_KEYRING.getDisplayName())
                                    .explanation(PRIVATE_KEYRING.getDisplayName() + " " + privateKeyring
                                            + " could not be opened with the provided " + PRIVATE_KEYRING_PASSPHRASE.getDisplayName())
                                    .build());
                        }
                    } catch (final Exception e) {
                        validationResults.add(new ValidationResult.Builder().subject(PRIVATE_KEYRING.getDisplayName())
                                .explanation("Invalid " + PRIVATE_KEYRING.getDisplayName() + " " + privateKeyring
                                        + " because " + e.toString())
                                .build());
                    }
                }
            }
        }

        return validationResults;
    }

    private List<ValidationResult> validatePBE(EncryptionMethod encryptionMethod, KeyDerivationFunction kdf, String password, boolean allowWeakCrypto) {
        List<ValidationResult> validationResults = new ArrayList<>();
        boolean limitedStrengthCrypto = !PasswordBasedEncryptor.supportsUnlimitedStrength();

        // Password required (short circuits validation because other conditions depend on password presence)
        if (StringUtils.isEmpty(password)) {
            validationResults.add(new ValidationResult.Builder().subject(PASSWORD.getName())
                    .explanation(PASSWORD.getDisplayName() + " is required when using algorithm " + encryptionMethod.getAlgorithm()).build());
            return validationResults;
        }

        // If weak crypto is not explicitly allowed via override, check the password length and algorithm
        final int passwordBytesLength = password.getBytes(StandardCharsets.UTF_8).length;
        if (!allowWeakCrypto) {
            final int minimumSafePasswordLength = PasswordBasedEncryptor.getMinimumSafePasswordLength();
            if (passwordBytesLength < minimumSafePasswordLength) {
                validationResults.add(new ValidationResult.Builder().subject(PASSWORD.getName())
                        .explanation("Password length less than " + minimumSafePasswordLength + " characters is potentially unsafe. See Admin Guide.").build());
            }
        }

        // Multiple checks on machine with limited strength crypto
        if (limitedStrengthCrypto) {
            // Cannot use unlimited strength ciphers on machine that lacks policies
            if (encryptionMethod.isUnlimitedStrength()) {
                validationResults.add(new ValidationResult.Builder().subject(ENCRYPTION_ALGORITHM.getName())
                        .explanation(encryptionMethod.name() + " (" + encryptionMethod.getAlgorithm() + ") is not supported by this JVM due to lacking JCE Unlimited " +
                                "Strength Jurisdiction Policy files. See Admin Guide.").build());
            }

            // Check if the password exceeds the limit
            final boolean passwordLongerThanLimit = !CipherUtility.passwordLengthIsValidForAlgorithmOnLimitedStrengthCrypto(passwordBytesLength, encryptionMethod);
            if (passwordLongerThanLimit) {
                int maxPasswordLength = CipherUtility.getMaximumPasswordLengthForAlgorithmOnLimitedStrengthCrypto(encryptionMethod);
                validationResults.add(new ValidationResult.Builder().subject(PASSWORD.getName())
                        .explanation("Password length greater than " + maxPasswordLength + " characters is not supported by this JVM" +
                                " due to lacking JCE Unlimited Strength Jurisdiction Policy files. See Admin Guide.").build());
            }
        }

        // Check the KDF for compatibility with this algorithm
        List<String> kdfsForPBECipher = getKDFsForPBECipher(encryptionMethod);
        if (kdf == null || !kdfsForPBECipher.contains(kdf.name())) {
            final String displayName = KEY_DERIVATION_FUNCTION.getDisplayName();
            validationResults.add(new ValidationResult.Builder().subject(displayName)
                    .explanation(displayName + " is required to be " + StringUtils.join(kdfsForPBECipher,
                            ", ") + " when using algorithm " + encryptionMethod.getAlgorithm() + ". See Admin Guide.").build());
        }

        return validationResults;
    }

    private List<ValidationResult> validateKeyed(EncryptionMethod encryptionMethod, KeyDerivationFunction kdf, String keyHex) {
        List<ValidationResult> validationResults = new ArrayList<>();
        boolean limitedStrengthCrypto = !PasswordBasedEncryptor.supportsUnlimitedStrength();

        if (limitedStrengthCrypto) {
            if (encryptionMethod.isUnlimitedStrength()) {
                validationResults.add(new ValidationResult.Builder().subject(ENCRYPTION_ALGORITHM.getName())
                        .explanation(encryptionMethod.name() + " (" + encryptionMethod.getAlgorithm() + ") is not supported by this JVM due to lacking JCE Unlimited " +
                                "Strength Jurisdiction Policy files. See Admin Guide.").build());
            }
        }
        int allowedKeyLength = PasswordBasedEncryptor.getMaxAllowedKeyLength(ENCRYPTION_ALGORITHM.getName());

        if (StringUtils.isEmpty(keyHex)) {
            validationResults.add(new ValidationResult.Builder().subject(RAW_KEY_HEX.getName())
                    .explanation(RAW_KEY_HEX.getDisplayName() + " is required when using algorithm " + encryptionMethod.getAlgorithm() + ". See Admin Guide.").build());
        } else {
            byte[] keyBytes = new byte[0];
            try {
                keyBytes = Hex.decodeHex(keyHex.toCharArray());
            } catch (DecoderException e) {
                validationResults.add(new ValidationResult.Builder().subject(RAW_KEY_HEX.getName())
                        .explanation("Key must be valid hexadecimal string. See Admin Guide.").build());
            }
            if (keyBytes.length * 8 > allowedKeyLength) {
                validationResults.add(new ValidationResult.Builder().subject(RAW_KEY_HEX.getName())
                        .explanation("Key length greater than " + allowedKeyLength + " bits is not supported by this JVM" +
                                " due to lacking JCE Unlimited Strength Jurisdiction Policy files. See Admin Guide.").build());
            }
            if (!CipherUtility.isValidKeyLengthForAlgorithm(keyBytes.length * 8, encryptionMethod.getAlgorithm())) {
                List<Integer> validKeyLengths = CipherUtility.getValidKeyLengthsForAlgorithm(encryptionMethod.getAlgorithm());
                validationResults.add(new ValidationResult.Builder().subject(RAW_KEY_HEX.getName())
                        .explanation("Key must be valid length [" + StringUtils.join(validKeyLengths, ", ") + "]. See Admin Guide.").build());
            }
        }

        // Perform some analysis on the selected encryption algorithm to ensure the JVM can support it and the associated key

        List<String> kdfsForKeyedCipher = getKDFsForKeyedCipher();
        if (kdf == null || !kdfsForKeyedCipher.contains(kdf.name())) {
            validationResults.add(new ValidationResult.Builder().subject(KEY_DERIVATION_FUNCTION.getName())
                    .explanation(KEY_DERIVATION_FUNCTION.getDisplayName() + " is required to be " + StringUtils.join(kdfsForKeyedCipher, ", ") + " when using algorithm " +
                            encryptionMethod.getAlgorithm()).build());
        }

        return validationResults;
    }

    private List<String> getKDFsForKeyedCipher() {
        List<String> kdfsForKeyedCipher = new ArrayList<>();
        kdfsForKeyedCipher.add(KeyDerivationFunction.NONE.name());
        for (KeyDerivationFunction k : KeyDerivationFunction.values()) {
            if (k.isStrongKDF()) {
                kdfsForKeyedCipher.add(k.name());
            }
        }
        return kdfsForKeyedCipher;
    }

    private List<String> getKDFsForPBECipher(EncryptionMethod encryptionMethod) {
        List<String> kdfsForPBECipher = new ArrayList<>();
        for (KeyDerivationFunction k : KeyDerivationFunction.values()) {
            // Add all weak (legacy) KDFs except NONE
            if (!k.isStrongKDF() && !k.equals(KeyDerivationFunction.NONE)) {
                kdfsForPBECipher.add(k.name());
                // If this algorithm supports strong KDFs, add them as well
            } else if ((encryptionMethod.isCompatibleWithStrongKDFs() && k.isStrongKDF())) {
                kdfsForPBECipher.add(k.name());
            }
        }
        return kdfsForPBECipher;
    }

    protected Map<String, String> buildNewAttributes(FlowFile file, String atrList,
                                                    EncryptContent.Encryptor encryptor,
                                                    boolean isToBeEncrypted) throws Exception {

        Map<String, String> oldAttributes = file.getAttributes();
        Map<String, String> atrToWrite = new HashMap<>();
        final String filenameAttr = CoreAttributes.FILENAME.key();
        final String uuidAttr = CoreAttributes.UUID.key();

        if (!StringUtils.isEmpty(atrList)) {
            // Traverse comma separated list if provided
            Set<String> atrSet = new HashSet<>(Arrays.asList(atrList));
            for (String atr : atrSet) {
                if (!atr.equals(filenameAttr) && !atr.equals(uuidAttr)) {
                    String atrValue = oldAttributes.get(atr);
                    String newAtrVal = (isToBeEncrypted) ? encryptor.getEncryptedString(atrValue) : encryptor.getDecryptedString(atrValue);
                    atrToWrite.put(atr, newAtrVal);
                }
            }
        } else {
            //TODO: filename encryption check only for PGP algos.
            // encrypt all attributes except filename and uuid.
            for (String atr : oldAttributes.keySet()) {
                if (!atr.equals(filenameAttr) && !atr.equals(uuidAttr)) {
                    String atrValue = oldAttributes.get(atr);
                    String newAtrVal = (isToBeEncrypted) ? encryptor.getEncryptedString(atrValue) : encryptor.getDecryptedString(atrValue);
                    atrToWrite.put(atr, newAtrVal);
                }
            }
        }
        return atrToWrite;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final String atrList = context.getProperty(ATTRIBUTES_TO_ENCRYPT).getValue();
        final String method = context.getProperty(ENCRYPTION_ALGORITHM).getValue();
        final EncryptionMethod encryptionMethod = EncryptionMethod.valueOf(method);
        final String providerName = encryptionMethod.getProvider();
        final String algorithm = encryptionMethod.getAlgorithm();
        final String password = context.getProperty(PASSWORD).getValue();
        final KeyDerivationFunction kdf = KeyDerivationFunction.valueOf(context.getProperty(KEY_DERIVATION_FUNCTION).getValue());
        final boolean encrypt = context.getProperty(MODE).getValue().equalsIgnoreCase(ENCRYPT_MODE);

        EncryptContent.Encryptor encryptor;
        Map<String, String> newAtrList;

        try {
            if (isPGPAlgorithm(algorithm)) {
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

            newAtrList = buildNewAttributes(flowFile, atrList, encryptor, encrypt);
            FlowFile newFlowFile  = session.putAllAttributes(flowFile,newAtrList);
            session.transfer(newFlowFile, REL_SUCCESS);

        } catch (final Exception e) {
            logger.error(e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }

    }
}
