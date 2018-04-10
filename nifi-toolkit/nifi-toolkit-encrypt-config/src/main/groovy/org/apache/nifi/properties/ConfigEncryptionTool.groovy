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
package org.apache.nifi.properties

import groovy.io.GroovyPrintWriter
import groovy.json.JsonBuilder
import groovy.xml.XmlUtil
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import org.apache.nifi.security.util.crypto.CipherUtility
import org.apache.nifi.security.util.crypto.scrypt.Scrypt
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException
import org.apache.nifi.toolkit.tls.commandLine.ExitCode
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.util.console.TextDevice
import org.apache.nifi.util.console.TextDevices
import org.bouncycastle.crypto.generators.SCrypt
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.xml.sax.SAXException

import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.PBEParameterSpec
import java.nio.charset.StandardCharsets
import java.security.InvalidKeyException
import java.security.KeyException
import java.security.MessageDigest
import java.security.SecureRandom
import java.security.Security
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

class ConfigEncryptionTool {
    private static final Logger logger = LoggerFactory.getLogger(ConfigEncryptionTool.class)

    public String bootstrapConfPath
    public String niFiPropertiesPath
    public String outputNiFiPropertiesPath
    public String loginIdentityProvidersPath
    public String outputLoginIdentityProvidersPath
    public String authorizersPath
    public String outputAuthorizersPath
    public String flowXmlPath
    public String outputFlowXmlPath
    // If this value can be set by the running user, it can point to a manipulated file anywhere
    private static String secureHashPath = "./secure_hash.key"

    private String keyHex
    private String migrationKeyHex
    private String password
    private String migrationPassword
    private String secureHashKey
    private String secureHashPassword

    // This is the raw value used in nifi.sensitive.props.key
    private String flowPropertiesPassword

    private String newFlowAlgorithm
    private String newFlowProvider

    private NiFiProperties niFiProperties
    private String loginIdentityProviders
    private String authorizers
    private String flowXml

    private boolean usingPassword = true
    private boolean usingPasswordMigration = true
    private boolean usingSecureHash = false
    private boolean migration = false
    private boolean isVerbose = false
    private boolean handlingNiFiProperties = false
    private boolean handlingLoginIdentityProviders = false
    private boolean handlingAuthorizers = false
    private boolean handlingFlowXml = false
    private boolean ignorePropertiesFiles = false
    private boolean queryingCurrentHashParams = false

    private static final String HELP_ARG = "help"
    private static final String VERBOSE_ARG = "verbose"
    private static final String BOOTSTRAP_CONF_ARG = "bootstrapConf"
    private static final String NIFI_PROPERTIES_ARG = "niFiProperties"
    private static final String OUTPUT_NIFI_PROPERTIES_ARG = "outputNiFiProperties"
    private static final String LOGIN_IDENTITY_PROVIDERS_ARG = "loginIdentityProviders"
    private static final String OUTPUT_LOGIN_IDENTITY_PROVIDERS_ARG = "outputLoginIdentityProviders"
    private static final String AUTHORIZERS_ARG = "authorizers"
    private static final String OUTPUT_AUTHORIZERS_ARG = "outputAuthorizers"
    private static final String FLOW_XML_ARG = "flowXml"
    private static final String OUTPUT_FLOW_XML_ARG = "outputFlowXml"
    private static final String KEY_ARG = "key"
    private static final String PASSWORD_ARG = "password"
    private static final String KEY_MIGRATION_ARG = "oldKey"
    private static final String PASSWORD_MIGRATION_ARG = "oldPassword"
    private static final String HASHED_KEY_MIGRATION_ARG = "secureHashKey"
    private static final String HASHED_PASSWORD_MIGRATION_ARG = "secureHashPassword"
    private static final String USE_KEY_ARG = "useRawKey"
    private static final String MIGRATION_ARG = "migrate"
    private static final String PROPS_KEY_ARG = "propsKey"
    private static final String DO_NOT_ENCRYPT_NIFI_PROPERTIES_ARG = "encryptFlowXmlOnly"
    private static final String NEW_FLOW_ALGORITHM_ARG = "newFlowAlgorithm"
    private static final String NEW_FLOW_PROVIDER_ARG = "newFlowProvider"
    private static final String CURRENT_HASH_PARAMS_ARG = "currentHashParams"

    // Static holder to avoid re-generating the options object multiple times in an invocation
    private static Options staticOptions

    // Hard-coded fallback value from {@link org.apache.nifi.encrypt.StringEncryptor}
    private static final String DEFAULT_NIFI_SENSITIVE_PROPS_KEY = "nififtw!"
    private static final int MIN_PASSWORD_LENGTH = 12

    // Strong parameters as of 12 Aug 2016 (for key derivation)
    // This value can remain an int until best practice specifies a value above 2**32
    private static final int SCRYPT_N = 2**16
    private static final int SCRYPT_R = 8
    private static final int SCRYPT_P = 1
    static final String CURRENT_SCRYPT_VERSION = "s0"
    private static final String NIFI_SCRYPT_PATTERN = /^\$\w{2}\$\w{5,}\$[\w\/\=\+]{22,}\$[\w\/\=\+]{43}$/

    // Hard-coded values from StandardPBEByteEncryptor which will be removed during refactor of all flow encryption code in NIFI-1465
    private static final int DEFAULT_KDF_ITERATIONS = 1000
    private static final int DEFAULT_SALT_SIZE_BYTES = 16

    private static
    final String BOOTSTRAP_KEY_COMMENT = "# Master key in hexadecimal format for encrypted sensitive configuration values"
    private static final String BOOTSTRAP_KEY_PREFIX = "nifi.bootstrap.sensitive.key="
    private static final String JAVA_HOME = "JAVA_HOME"
    private static final String NIFI_TOOLKIT_HOME = "NIFI_TOOLKIT_HOME"
    private static final String SEP = System.lineSeparator()

    private static final String FOOTER = buildFooter()

    private static
    final String DEFAULT_DESCRIPTION = "This tool reads from a nifi.properties and/or " +
            "login-identity-providers.xml file with plain sensitive configuration values, " +
            "prompts the user for a master key, and encrypts each value. It will replace the " +
            "plain value with the protected value in the same file (or write to a new file if " +
            "specified). It can also be used to migrate already-encrypted values in those " +
            "files or in flow.xml.gz to be encrypted with a new key."

    private static final String LDAP_PROVIDER_CLASS = "org.apache.nifi.ldap.LdapProvider"
    private static
    final String LDAP_PROVIDER_REGEX = /(?s)<provider>(?:(?!<provider>).)*?<class>\s*org\.apache\.nifi\.ldap\.LdapProvider.*?<\/provider>/
    /* Explanation of LDAP_PROVIDER_REGEX:
     *   (?s)                             -> single-line mode (i.e., `.` in regex matches newlines)
     *   <provider>                       -> find occurrence of `<provider>` literally (case-sensitive)
     *   (?: ... )                        -> group but do not capture submatch
     *   (?! ... )                        -> negative lookahead
     *   (?:(?!<provider>).)*?            -> find everything until a new `<provider>` starts. This is for not selecting multiple providers in one match
     *   <class>                          -> find occurrence of `<class>` literally (case-sensitive)
     *   \s*                              -> find any whitespace
     *   org\.apache\.nifi\.ldap\.LdapProvider
     *                                    -> find occurrence of `org.apache.nifi.ldap.LdapProvider` literally (case-sensitive)
     *   .*?</provider>                   -> find everything as needed up until and including occurrence of `</provider>`
     */

    private static final String LDAP_USER_GROUP_PROVIDER_CLASS = "org.apache.nifi.ldap.tenants.LdapUserGroupProvider"
    private static final String LDAP_USER_GROUP_PROVIDER_REGEX =
            /(?s)<userGroupProvider>(?:(?!<userGroupProvider>).)*?<class>\s*org\.apache\.nifi\.ldap\.tenants\.LdapUserGroupProvider.*?<\/userGroupProvider>/
    /* Explanation of LDAP_USER_GROUP_PROVIDER_REGEX:
     *   (?s)                             -> single-line mode (i.e., `.` in regex matches newlines)
     *   <userGroupProvider>              -> find occurrence of `<userGroupProvider>` literally (case-sensitive)
     *   (?: ... )                        -> group but do not capture submatch
     *   (?! ... )                        -> negative lookahead
     *   (?:(?!<userGroupProvider>).)*?   -> find everything until a new `<userGroupProvider>` starts. This is for not selecting multiple userGroupProviders in one match
     *   <class>                          -> find occurrence of `<class>` literally (case-sensitive)
     *   \s*                              -> find any whitespace
     *   org\.apache\.nifi\.ldap\.tenants\.LdapUserGroupProvider
     *                                    -> find occurrence of `org.apache.nifi.ldap.tenants.LdapUserGroupProvider` literally (case-sensitive)
     *   .*?</userGroupProvider>          -> find everything as needed up until and including occurrence of '</userGroupProvider>'
     */

    private static final String XML_DECLARATION_REGEX = /<\?xml version="1.0" encoding="UTF-8"\?>/
    private static final String WRAPPED_FLOW_XML_CIPHER_TEXT_REGEX = /enc\{[a-fA-F0-9]+?\}/

    private static final String DEFAULT_PROVIDER = BouncyCastleProvider.PROVIDER_NAME
    private static final String DEFAULT_FLOW_ALGORITHM = "PBEWITHMD5AND256BITAES-CBC-OPENSSL"
    static private final int AMBARI_COMPATIBLE_SCRYPT_HASH_LENGTH = 256

    private static String buildHeader(String description = DEFAULT_DESCRIPTION) {
        "${SEP}${description}${SEP * 2}"
    }

    private static String buildFooter() {
        "${SEP}Java home: ${System.getenv(JAVA_HOME)}${SEP}NiFi Toolkit home: ${System.getenv(NIFI_TOOLKIT_HOME)}"
    }

    private final Options options
    private final String header


    ConfigEncryptionTool() {
        this(DEFAULT_DESCRIPTION)
    }

    ConfigEncryptionTool(String description) {
        this.header = buildHeader(description)
        this.options = getCliOptions()
    }

    static Options buildOptions() {
        Options options = new Options()
        options.addOption(Option.builder("h").longOpt(HELP_ARG).hasArg(false).desc("Show usage information (this message)").build())
        options.addOption(Option.builder("v").longOpt(VERBOSE_ARG).hasArg(false).desc("Sets verbose mode (default false)").build())
        options.addOption(Option.builder("n").longOpt(NIFI_PROPERTIES_ARG).hasArg(true).argName("file").desc("The nifi.properties file containing unprotected config values (will be overwritten unless -o is specified)").build())
        options.addOption(Option.builder("o").longOpt(OUTPUT_NIFI_PROPERTIES_ARG).hasArg(true).argName("file").desc("The destination nifi.properties file containing protected config values (will not modify input nifi.properties)").build())
        options.addOption(Option.builder("l").longOpt(LOGIN_IDENTITY_PROVIDERS_ARG).hasArg(true).argName("file").desc("The login-identity-providers.xml file containing unprotected config values (will be overwritten unless -i is specified)").build())
        options.addOption(Option.builder("i").longOpt(OUTPUT_LOGIN_IDENTITY_PROVIDERS_ARG).hasArg(true).argName("file").desc("The destination login-identity-providers.xml file containing protected config values (will not modify input login-identity-providers.xml)").build())
        options.addOption(Option.builder("a").longOpt(AUTHORIZERS_ARG).hasArg(true).argName("file").desc("The authorizers.xml file containing unprotected config values (will be overwritten unless -u is specified)").build())
        options.addOption(Option.builder("u").longOpt(OUTPUT_AUTHORIZERS_ARG).hasArg(true).argName("file").desc("The destination authorizers.xml file containing protected config values (will not modify input authorizers.xml)").build())
        options.addOption(Option.builder("f").longOpt(FLOW_XML_ARG).hasArg(true).argName("file").desc("The flow.xml.gz file currently protected with old password (will be overwritten unless -g is specified)").build())
        options.addOption(Option.builder("g").longOpt(OUTPUT_FLOW_XML_ARG).hasArg(true).argName("file").desc("The destination flow.xml.gz file containing protected config values (will not modify input flow.xml.gz)").build())
        options.addOption(Option.builder("b").longOpt(BOOTSTRAP_CONF_ARG).hasArg(true).argName("file").desc("The bootstrap.conf file to persist master key").build())
        options.addOption(Option.builder("k").longOpt(KEY_ARG).hasArg(true).argName("keyhex").desc("The raw hexadecimal key to use to encrypt the sensitive properties").build())
        options.addOption(Option.builder("e").longOpt(KEY_MIGRATION_ARG).hasArg(true).argName("keyhex").desc("The old raw hexadecimal key to use during key migration").build())
        options.addOption(Option.builder("p").longOpt(PASSWORD_ARG).hasArg(true).argName("password").desc("The password from which to derive the key to use to encrypt the sensitive properties").build())
        options.addOption(Option.builder("w").longOpt(PASSWORD_MIGRATION_ARG).hasArg(true).argName("password").desc("The old password from which to derive the key during migration").build())
        options.addOption(Option.builder("y").longOpt(HASHED_KEY_MIGRATION_ARG).hasArg(true).argName("hashed_keyhex").desc("The old securely-hashed hexadecimal key to authenticate during key migration (see NiFi Admin Guide)").build())
        options.addOption(Option.builder("z").longOpt(HASHED_PASSWORD_MIGRATION_ARG).hasArg(true).argName("hashed_password").desc("The old securely-hashed password to authenticate during key migration (see NiFi Admin Guide)").build())
        options.addOption(Option.builder("r").longOpt(USE_KEY_ARG).hasArg(false).desc("If provided, the secure console will prompt for the raw key value in hexadecimal form").build())
        options.addOption(Option.builder("m").longOpt(MIGRATION_ARG).hasArg(false).desc("If provided, the nifi.properties and/or login-identity-providers.xml sensitive properties will be re-encrypted with a new key").build())
        options.addOption(Option.builder("x").longOpt(DO_NOT_ENCRYPT_NIFI_PROPERTIES_ARG).hasArg(false).desc("If provided, the properties in flow.xml.gz will be re-encrypted with a new key but the nifi.properties and/or login-identity-providers.xml files will not be modified").build())
        options.addOption(Option.builder("s").longOpt(PROPS_KEY_ARG).hasArg(true).argName("password|keyhex").desc("The password or key to use to encrypt the sensitive processor properties in flow.xml.gz").build())
        options.addOption(Option.builder("A").longOpt(NEW_FLOW_ALGORITHM_ARG).hasArg(true).argName("algorithm").desc("The algorithm to use to encrypt the sensitive processor properties in flow.xml.gz").build())
        options.addOption(Option.builder("P").longOpt(NEW_FLOW_PROVIDER_ARG).hasArg(true).argName("algorithm").desc("The security provider to use to encrypt the sensitive processor properties in flow.xml.gz").build())
        options.addOption(Option.builder().longOpt(CURRENT_HASH_PARAMS_ARG).hasArg(false).desc("Returns the current salt and cost params used to store the hashed key/password").build())
        options
    }

    static Options getCliOptions() {
        if (!staticOptions) {
            staticOptions = buildOptions()
        }
        return staticOptions
    }

/**
 * Prints the usage message and available arguments for this tool (along with a specific error message if provided).
 *
 * @param errorMessage the optional error message
 */
    void printUsage(String errorMessage) {
        if (errorMessage) {
            System.out.println(errorMessage)
            System.out.println()
        }
        HelpFormatter helpFormatter = new HelpFormatter()
        helpFormatter.setWidth(160)
        helpFormatter.setOptionComparator(null)
        // preserve manual ordering of options when printing instead of alphabetical
        helpFormatter.printHelp(ConfigEncryptionTool.class.getCanonicalName(), header, options, FOOTER, true)
    }

    protected void printUsageAndThrow(String errorMessage, ExitCode exitCode) throws CommandLineParseException {
        printUsage(errorMessage)
        throw new CommandLineParseException(errorMessage, exitCode)
    }

    // TODO: Refactor component steps into methods
    protected CommandLine parse(String[] args) throws CommandLineParseException {
        CommandLineParser parser = new DefaultParser()
        CommandLine commandLine
        try {
            commandLine = parser.parse(options, args)
            if (commandLine.hasOption(HELP_ARG)) {
                printUsageAndThrow(null, ExitCode.HELP)
            }

            isVerbose = commandLine.hasOption(VERBOSE_ARG)

            // If this flag is present, ensure no other options are present and then fail/return
            if (commandLine.hasOption(CURRENT_HASH_PARAMS_ARG)) {
                queryingCurrentHashParams = true
                if (commandLineHasActionFlags(commandLine, [CURRENT_HASH_PARAMS_ARG])) {
                    printUsageAndThrow("When '--${CURRENT_HASH_PARAMS_ARG}' is specified, only '-h'/'--${HELP_ARG}' and '-v'/'--${VERBOSE_ARG}' are allowed", ExitCode.INVALID_ARGS)
                } else {
                    // Otherwise return (avoid unnecessary parsing)
                    return commandLine
                }
            }

            bootstrapConfPath = commandLine.getOptionValue(BOOTSTRAP_CONF_ARG)

            // If this flag is provided, the nifi.properties is necessary to read/write the flow encryption key, but the encryption process will not actually be applied to nifi.properties / login-identity-providers.xml
            if (commandLine.hasOption(DO_NOT_ENCRYPT_NIFI_PROPERTIES_ARG)) {
                handlingNiFiProperties = false
                handlingLoginIdentityProviders = false
                handlingAuthorizers = false
                ignorePropertiesFiles = true
            } else {
                if (commandLine.hasOption(LOGIN_IDENTITY_PROVIDERS_ARG)) {
                    if (isVerbose) {
                        logger.info("Handling encryption of login-identity-providers.xml")
                    }
                    loginIdentityProvidersPath = commandLine.getOptionValue(LOGIN_IDENTITY_PROVIDERS_ARG)
                    outputLoginIdentityProvidersPath = commandLine.getOptionValue(OUTPUT_LOGIN_IDENTITY_PROVIDERS_ARG, loginIdentityProvidersPath)
                    handlingLoginIdentityProviders = true

                    if (loginIdentityProvidersPath == outputLoginIdentityProvidersPath) {
                        // TODO: Add confirmation pause and provide -y flag to offer no-interaction mode?
                        logger.warn("The source login-identity-providers.xml and destination login-identity-providers.xml are identical [${outputLoginIdentityProvidersPath}] so the original will be overwritten")
                    }
                }
                if (commandLine.hasOption(AUTHORIZERS_ARG)) {
                    if (isVerbose) {
                        logger.info("Handling encryption of authorizers.xml")
                    }
                    authorizersPath = commandLine.getOptionValue(AUTHORIZERS_ARG)
                    outputAuthorizersPath = commandLine.getOptionValue(OUTPUT_AUTHORIZERS_ARG, authorizersPath)
                    handlingAuthorizers = true

                    if (authorizersPath == outputAuthorizersPath) {
                        // TODO: Add confirmation pause and provide -y flag to offer no-interaction mode?
                        logger.warn("The source authorizers.xml and destination authorizers.xml are identical [${outputAuthorizersPath}] so the original will be overwritten")
                    }
                }
            }

            // This needs to occur even if the nifi.properties won't be encrypted
            if (commandLine.hasOption(NIFI_PROPERTIES_ARG)) {
                boolean ignoreFlagPresent = commandLine.hasOption(DO_NOT_ENCRYPT_NIFI_PROPERTIES_ARG)
                if (isVerbose && !ignoreFlagPresent) {
                    logger.info("Handling encryption of nifi.properties")
                }
                niFiPropertiesPath = commandLine.getOptionValue(NIFI_PROPERTIES_ARG)
                outputNiFiPropertiesPath = commandLine.getOptionValue(OUTPUT_NIFI_PROPERTIES_ARG, niFiPropertiesPath)
                handlingNiFiProperties = !ignoreFlagPresent

                if (niFiPropertiesPath == outputNiFiPropertiesPath) {
                    // TODO: Add confirmation pause and provide -y flag to offer no-interaction mode?
                    logger.warn("The source nifi.properties and destination nifi.properties are identical [${outputNiFiPropertiesPath}] so the original will be overwritten")
                }
            }

            if (commandLine.hasOption(FLOW_XML_ARG)) {
                if (isVerbose) {
                    logger.info("Handling encryption of flow.xml.gz")
                }
                flowXmlPath = commandLine.getOptionValue(FLOW_XML_ARG)
                outputFlowXmlPath = commandLine.getOptionValue(OUTPUT_FLOW_XML_ARG, flowXmlPath)
                handlingFlowXml = true

                newFlowAlgorithm = commandLine.getOptionValue(NEW_FLOW_ALGORITHM_ARG)
                newFlowProvider = commandLine.getOptionValue(NEW_FLOW_PROVIDER_ARG)

                if (flowXmlPath == outputFlowXmlPath) {
                    // TODO: Add confirmation pause and provide -y flag to offer no-interaction mode?
                    logger.warn("The source flow.xml.gz and destination flow.xml.gz are identical [${outputFlowXmlPath}] so the original will be overwritten")
                }

                if (!commandLine.hasOption(NIFI_PROPERTIES_ARG)) {
                    printUsageAndThrow("In order to migrate a flow.xml.gz, a nifi.properties file must also be specified via '-n'/'--${NIFI_PROPERTIES_ARG}'.", ExitCode.INVALID_ARGS)
                }
            }

            if (isVerbose) {
                logger.info("       bootstrap.conf:               ${bootstrapConfPath}")
                logger.info("(src)  nifi.properties:              ${niFiPropertiesPath}")
                logger.info("(dest) nifi.properties:              ${outputNiFiPropertiesPath}")
                logger.info("(src)  login-identity-providers.xml: ${loginIdentityProvidersPath}")
                logger.info("(dest) login-identity-providers.xml: ${outputLoginIdentityProvidersPath}")
                logger.info("(src)  authorizers.xml:              ${authorizersPath}")
                logger.info("(dest) authorizers.xml:              ${outputAuthorizersPath}")
                logger.info("(src)  flow.xml.gz:                  ${flowXmlPath}")
                logger.info("(dest) flow.xml.gz:                  ${outputFlowXmlPath}")
            }

            if (!commandLine.hasOption(NIFI_PROPERTIES_ARG)
                    && !commandLine.hasOption(LOGIN_IDENTITY_PROVIDERS_ARG)
                    && !commandLine.hasOption(AUTHORIZERS_ARG)
                    && !commandLine.hasOption(DO_NOT_ENCRYPT_NIFI_PROPERTIES_ARG)
            ) {
                printUsageAndThrow("One or more of [" +
                        "'-n'/'--${NIFI_PROPERTIES_ARG}', " +
                        "'-l'/'--${LOGIN_IDENTITY_PROVIDERS_ARG}', " +
                        "'-a'/'--${AUTHORIZERS_ARG}'" +
                        "] must be provided unless " +
                        "'-x'/--'${DO_NOT_ENCRYPT_NIFI_PROPERTIES_ARG}' is specified", ExitCode.INVALID_ARGS)
            }

            if (commandLine.hasOption(MIGRATION_ARG)) {
                migration = true
                if (isVerbose) {
                    logger.info("Key migration mode activated")
                }
                if (isSecureHashArgumentPresent(commandLine)) {
                    logger.info("Secure hash argument present")

                    // Check for old key/password and throw error
                    if (commandLine.hasOption(KEY_MIGRATION_ARG) || commandLine.hasOption(PASSWORD_MIGRATION_ARG)) {
                        printUsageAndThrow("If the '-w'/'--${PASSWORD_MIGRATION_ARG}' or '-e'/'--${KEY_MIGRATION_ARG}' arguments are present, '-z'/'--${HASHED_PASSWORD_MIGRATION_ARG}' and '-y'/'--${HASHED_KEY_MIGRATION_ARG}' cannot be used", ExitCode.INVALID_ARGS)
                    }

                    // Check for both key and password and throw error
                    if (commandLine.hasOption(HASHED_KEY_MIGRATION_ARG) && commandLine.hasOption(HASHED_PASSWORD_MIGRATION_ARG)) {
                        printUsageAndThrow("Only one of '-z'/'--${HASHED_PASSWORD_MIGRATION_ARG}' and '-y'/'--${HASHED_KEY_MIGRATION_ARG}' can be used together", ExitCode.INVALID_ARGS)
                    }

                    // Extract flags to field
                    if (commandLine.hasOption(HASHED_KEY_MIGRATION_ARG)) {
                        secureHashKey = commandLine.getOptionValue(HASHED_KEY_MIGRATION_ARG)
                    } else {
                        secureHashPassword = commandLine.getOptionValue(HASHED_PASSWORD_MIGRATION_ARG)
                    }

                    // Set boolean flag to true
                    usingSecureHash = true
                }
                if (commandLine.hasOption(PASSWORD_MIGRATION_ARG)) {
                    usingPasswordMigration = true
                    if (commandLine.hasOption(KEY_MIGRATION_ARG)) {
                        printUsageAndThrow("Only one of '-w'/'--${PASSWORD_MIGRATION_ARG}' and '-e'/'--${KEY_MIGRATION_ARG}' can be used", ExitCode.INVALID_ARGS)
                    } else {
                        migrationPassword = commandLine.getOptionValue(PASSWORD_MIGRATION_ARG)
                    }
                } else {
                    migrationKeyHex = commandLine.getOptionValue(KEY_MIGRATION_ARG)
                    // Use the "migration password" value if the migration key hex is absent and the secure hash password/key hex is absent (if either are present, the migration password is not)
                    usingPasswordMigration = !migrationKeyHex && !usingSecureHash
                }
            } else {
                if (commandLine.hasOption(PASSWORD_MIGRATION_ARG) || commandLine.hasOption(KEY_MIGRATION_ARG)) {
                    printUsageAndThrow("'-w'/'--${PASSWORD_MIGRATION_ARG}' and '-e'/'--${KEY_MIGRATION_ARG}' are ignored unless '-m'/'--${MIGRATION_ARG}' is enabled", ExitCode.INVALID_ARGS)
                }
            }

            if (commandLine.hasOption(PASSWORD_ARG)) {
                usingPassword = true
                if (commandLine.hasOption(KEY_ARG)) {
                    printUsageAndThrow("Only one of '-p'/'--${PASSWORD_ARG}' and '-k'/'--${KEY_ARG}' can be used", ExitCode.INVALID_ARGS)
                } else {
                    password = commandLine.getOptionValue(PASSWORD_ARG)
                }
            } else {
                keyHex = commandLine.getOptionValue(KEY_ARG)
                usingPassword = !keyHex
            }

            if (commandLine.hasOption(USE_KEY_ARG)) {
                if (keyHex || password) {
                    logger.warn("If the key or password is provided in the arguments, '-r'/'--${USE_KEY_ARG}' is ignored")
                } else {
                    usingPassword = false
                }
            }

            if (commandLine.hasOption(PROPS_KEY_ARG)) {
                flowPropertiesPassword = commandLine.getOptionValue(PROPS_KEY_ARG)
            }
        } catch (ParseException e) {
            if (isVerbose) {
                logger.error("Encountered an error", e)
            }
            printUsageAndThrow("Error parsing command line. (" + e.getMessage() + ")", ExitCode.ERROR_PARSING_COMMAND_LINE)
        }
        return commandLine
    }

    boolean commandLineHasActionFlags(CommandLine commandLine, List<String> acceptableOptionStrings = []) {
        // Resolve the list of Option objects corresponding to "help" and "verbose"
        final List<Option> ALWAYS_ACCEPTABLE_OPTIONS = resolveOptions([HELP_ARG, VERBOSE_ARG])

        // Resolve the list of Option objects corresponding to the provided "additional acceptable options"
        List<Option> acceptableOptions = resolveOptions(acceptableOptionStrings)

        // Determine the options submitted to the command line that are not acceptable
        List<Option> invalidOptions = commandLine.options - (acceptableOptions + ALWAYS_ACCEPTABLE_OPTIONS)
        if (invalidOptions) {
            if (isVerbose) {
                logger.error("In this mode, the following options are invalid: ${invalidOptions}")
            }
            return true
        } else {
            return false
        }
    }

    static List<Option> resolveOptions(List<String> strings) {
        strings?.collect { String opt ->
            getCliOptions().options.find { it.opt == opt || it.longOpt == opt }
        }
    }

    static boolean isSecureHashArgumentPresent(CommandLine commandLine) {
        commandLine.hasOption(HASHED_PASSWORD_MIGRATION_ARG) || commandLine.hasOption(HASHED_KEY_MIGRATION_ARG)
    }
/**
 * The method returns the provided, derived, or securely-entered key in hex format. The reason the parameters must be provided instead of read from the fields is because this is used for the regular key/password and the migration key/password.
 *
 * @param device
 * @param keyHex
 * @param password
 * @param usingPassword
 * @return
 */
    private String getKeyInternal(TextDevice device = TextDevices.defaultTextDevice(), String keyHex, String password, boolean usingPassword) {
        if (usingPassword) {
            if (!password) {
                if (isVerbose) {
                    logger.info("Reading password from secure console")
                }
                password = readPasswordFromConsole(device)
            }
            keyHex = deriveKeyFromPassword(password)
            password = null
            return keyHex
        } else {
            if (!keyHex) {
                if (isVerbose) {
                    logger.info("Reading hex key from secure console")
                }
                keyHex = readKeyFromConsole(device)
            }
            return keyHex
        }
    }

    private String getKey(TextDevice textDevice = TextDevices.defaultTextDevice()) {
        getKeyInternal(textDevice, keyHex, password, usingPassword)
    }

    private String getMigrationKey() {
        if (usingSecureHash) {
            // The boolean flag for "key" means the expression should evaluate to true when key is present and password is not
            String knownHashValue = readSecureHashValueFromFile(secureHashKey && !secureHashPassword)
            if (checkHashedValue(knownHashValue, getProvidedSecureHashValue())) {
                // Retrieve the key from bootstrap.conf because the caller only has the hashed version available
                return readMasterKeyFromBootstrap()
            } else {
                throw new InvalidKeyException("The provided hashed key/password is not correct")
            }
        } else {
            return getKeyInternal(TextDevices.defaultTextDevice(), migrationKeyHex, migrationPassword, usingPasswordMigration)
        }
    }

    private String getProvidedSecureHashValue() {
        if (usingSecureHash) {
            return secureHashPassword ?: secureHashKey
        } else {
            return ""
        }
    }

    private static String readSecureHashValueFromFile(boolean readKey = true) {
        File secureHashFile = new File(secureHashPath)
        if (!secureHashFile.canRead()) {
            throw new IOException("Cannot read from secure hash file")
        }
        List<String> lines = secureHashFile.readLines()
        String linePrefix = readKey ? "secureHashKey" : "secureHashPassword"
        String relevantLine = lines.find { it.startsWith(linePrefix) }
        String hashValue = relevantLine?.split("=")?.last()
        if (!hashValue) {
            throw new InvalidKeyException("The secure hash of the ${readKey ? "key" : "password"} could not be read from the stored file")
        } else {
            return hashValue
        }
    }

    /**
     * Returns true if the hash values are equivalent. Currently performs a *constant-time equality* check on the two values. As the scrypt format does not allow for reversing, two "equivalent" but non-identical hash values cannot be compared for equality.
     *
     * Example (byte equivalent):
     *
     * KHV: {@code $s0$40801$AAAAAAAAAAAAAAAAAAAAAA$gLSh7ChbHdOIMvZ74XGjV6qF65d9qvQ8n75FeGnM8YM}
     * UHV: {@code $s0$40801$AAAAAAAAAAAAAAAAAAAAAA$gLSh7ChbHdOIMvZ74XGjV6qF65d9qvQ8n75FeGnM8YM}
     * Result: EQUAL
     *
     * Example (semantically equivalent):
     *
     * KHV: {@code $s0$40801$AAAAAAAAAAAAAAAAAAAAAA$gLSh7ChbHdOIMvZ74XGjV6qF65d9qvQ8n75FeGnM8YM}
     * UHV: {@code $s0$40801$ABCDEFGHIJKLMNOPQRSTUQ$hxU5g0eH6sRkBqcsiApI8jxvKRT+2QMCenV0GToiMQ8}
     * Result: NOT EQUAL
     *
     * Even though both hash values are the result of the input "password", by design the hash values cannot be reversed to a common origin to determine their equality. If the raw input was known, both hashes could be determined to be valid, thus asserting the correctness of the raw input and the functional equivalence of the hash values.
     *
     * @param knownHashValue
     * @param unknownHashValue
     * @return true if the hash values are present and equal
     */
    static boolean checkHashedValue(String knownHashValue, String unknownHashValue) {
        if (!knownHashValue || !unknownHashValue) {
            return false
        }

        // The values should be in scrypt format
        if (!verifyHashFormat(knownHashValue) || !verifyHashFormat(unknownHashValue)) {
            return false
        }

        return MessageDigest.isEqual(knownHashValue.getBytes(StandardCharsets.UTF_8), unknownHashValue.getBytes(StandardCharsets.UTF_8))
    }

    /**
     * Returns true if the provided hash is in the valid format for NiFi secured hash storage. NiFi enforces additional constraints over the minimum Scrypt requirements (16+ byte [22+ B64] salt, 32 byte [43 B64] hash).
     *
     * @param hash the hash to verify
     * @return true if the format is acceptable
     */
    static boolean verifyHashFormat(String hash) {
        hash =~ NIFI_SCRYPT_PATTERN
    }

    private static String getFlowPassword(TextDevice textDevice = TextDevices.defaultTextDevice()) {
        readPasswordFromConsole(textDevice)
    }

    private static String readKeyFromConsole(TextDevice textDevice) {
        textDevice.printf("Enter the master key in hexadecimal format (spaces acceptable): ")
        new String(textDevice.readPassword())
    }

    private static String readPasswordFromConsole(TextDevice textDevice) {
        textDevice.printf("Enter the password: ")
        new String(textDevice.readPassword())
    }

    private String readMasterKeyFromBootstrap() {
        NiFiPropertiesLoader.extractKeyFromBootstrapFile(bootstrapConfPath)
    }

    /**
     * Returns the key in uppercase hexadecimal format with delimiters (spaces, '-', etc.) removed. All non-hex chars are removed. If the result is not a valid length (32, 48, 64 chars depending on the JCE), an exception is thrown.
     *
     * @param rawKey the unprocessed key input
     * @return the formatted hex string in uppercase
     * @throws KeyException if the key is not a valid length after parsing
     */
    private static String parseKey(String rawKey) throws KeyException {
        String hexKey = rawKey.replaceAll("[^0-9a-fA-F]", "")
        def validKeyLengths = getValidKeyLengths()
        if (!validKeyLengths.contains(hexKey.size() * 4)) {
            throw new KeyException("The key (${hexKey.size()} hex chars) must be of length ${validKeyLengths} bits (${validKeyLengths.collect { it / 4 }} hex characters)")
        }
        hexKey.toUpperCase()
    }

    /**
     * Returns the list of acceptable key lengths in bits based on the current JCE policies.
     *
     * @return 128 , [192, 256]
     */
    static List<Integer> getValidKeyLengths() {
        Cipher.getMaxAllowedKeyLength("AES") > 128 ? [128, 192, 256] : [128]
    }

    /**
     * Loads the {@link NiFiProperties} instance from the provided file path (restoring the original value of the System property {@code nifi.properties.file.path} after loading this instance).
     *
     * @return the NiFiProperties instance
     * @throw IOException if the nifi.properties file cannot be read
     */
    private NiFiProperties loadNiFiProperties(String existingKeyHex = keyHex) throws IOException {
        File niFiPropertiesFile
        if (niFiPropertiesPath && (niFiPropertiesFile = new File(niFiPropertiesPath)).exists()) {
            NiFiProperties properties
            try {
                properties = NiFiPropertiesLoader.withKey(existingKeyHex).load(niFiPropertiesFile)
                logger.info("Loaded NiFiProperties instance with ${properties.size()} properties")
                return properties
            } catch (RuntimeException e) {
                if (isVerbose) {
                    logger.error("Encountered an error", e)
                }
                throw new IOException("Cannot load NiFiProperties from [${niFiPropertiesPath}]", e)
            }
        } else {
            printUsageAndThrow("Cannot load NiFiProperties from [${niFiPropertiesPath}]", ExitCode.ERROR_READING_NIFI_PROPERTIES)
        }
    }

    /**
     * Loads the login identity providers configuration from the provided file path.
     *
     * @param existingKeyHex the key used to encrypt the configs (defaults to the current key)
     *
     * @return the file content
     * @throw IOException if the login-identity-providers.xml file cannot be read
     */
    private String loadLoginIdentityProviders(String existingKeyHex = keyHex) throws IOException {
        File loginIdentityProvidersFile
        if (loginIdentityProvidersPath && (loginIdentityProvidersFile = new File(loginIdentityProvidersPath)).exists()) {
            try {
                List<String> lines = loginIdentityProvidersFile.readLines()
                String xmlContent = lines.join("\n")
                logger.info("Loaded login identity providers content (${lines.size()} lines)")
                String decryptedXmlContent = decryptLoginIdentityProviders(xmlContent, existingKeyHex)
                return decryptedXmlContent
            } catch (RuntimeException e) {
                if (isVerbose) {
                    logger.error("Encountered an error", e)
                }
                throw new IOException("Cannot load login identity providers from [${loginIdentityProvidersPath}]", e)
            }
        } else {
            printUsageAndThrow("Cannot load login identity providers from [${loginIdentityProvidersPath}]", ExitCode.ERROR_READING_NIFI_PROPERTIES)
        }
    }

    /**
     * Loads the authorizers configuration from the provided file path.
     *
     * @param existingKeyHex the key used to encrypt the configs (defaults to the current key)
     *
     * @return the file content
     * @throw IOException if the authorizers.xml file cannot be read
     */
    private String loadAuthorizers(String existingKeyHex = keyHex) throws IOException {
        File authorizersFile
        if (authorizersPath && (authorizersFile = new File(authorizersPath)).exists()) {
            try {
                List<String> lines = authorizersFile.readLines()
                String xmlContent = lines.join("\n")
                logger.info("Loaded authorizers content (${lines.size()} lines)")
                String decryptedXmlContent = decryptAuthorizers(xmlContent, existingKeyHex)
                return decryptedXmlContent
            } catch (RuntimeException e) {
                if (isVerbose) {
                    logger.error("Encountered an error", e)
                }
                throw new IOException("Cannot load authorizers from [${authorizersPath}]", e)
            }
        } else {
            printUsageAndThrow("Cannot load authorizers from [${authorizersPath}]", ExitCode.ERROR_READING_NIFI_PROPERTIES)
        }
    }

    /**
     * Loads the flow definition from the provided file path, handling the GZIP file compression. Unlike {@link #loadLoginIdentityProviders()} this method does not decrypt the content (for performance and separation of concern reasons).
     *
     * @return the file content
     * @throw IOException if the flow.xml.gz file cannot be read
     */
    private String loadFlowXml() throws IOException {
        if (flowXmlPath && (new File(flowXmlPath)).exists()) {
            try {
                new FileInputStream(flowXmlPath).withCloseable {
                    new GZIPInputStream(it).withCloseable {
                        String xmlContent = IOUtils.toString(it, StandardCharsets.UTF_8)
                        return xmlContent
                    }
                }
            } catch (RuntimeException e) {
                if (isVerbose) {
                    logger.error("Encountered an error", e)
                }
                throw new IOException("Cannot load flow from [${flowXmlPath}]", e)
            }
        } else {
            printUsageAndThrow("Cannot load flow from [${flowXmlPath}]", ExitCode.ERROR_READING_NIFI_PROPERTIES)
        }
    }

    /**
     * Decrypts a single element encrypted in the flow.xml.gz style (hex-encoded and wrapped with "enc{" and "}").
     *
     * Example:
     * {@code enc{0123456789ABCDEF} } -> "some text"
     *
     * @param wrappedCipherText the wrapped and hex-encoded cipher text
     * @param password the password used to encrypt the content (UTF-8 encoded)
     * @param algorithm the encryption and KDF algorithm (defaults to PBEWITHMD5AND256BITAES-CBC-OPENSSL)
     * @param provider the security provider (defaults to BC)
     * @return the plaintext in UTF-8 encoding
     */
    private
    static String decryptFlowElement(String wrappedCipherText, String password, String algorithm = DEFAULT_FLOW_ALGORITHM, String provider = DEFAULT_PROVIDER) {
        // Drop the "enc{" and closing "}"
        if (!(wrappedCipherText =~ WRAPPED_FLOW_XML_CIPHER_TEXT_REGEX)) {
            throw new SensitivePropertyProtectionException("The provided cipher text does not match the expected format 'enc{0123456789ABCDEF...}'")
        }
        String unwrappedCipherText = wrappedCipherText.replaceAll(/enc\{/, "")[0..<-1]
        if (unwrappedCipherText.length() % 2 == 1 || unwrappedCipherText.length() == 0) {
            throw new SensitivePropertyProtectionException("The provided cipher text must have an even number of hex characters")
        }

        // Decode the hex
        byte[] cipherBytes = Hex.decodeHex(unwrappedCipherText.chars)

        /* The structure of each cipher text is 16 bytes of salt || actual cipher text,
         * so extract the salt (32 bytes encoded as hex, 16 bytes raw) and combine that
         * with the default (and unchanged) iteration count that is hardcoded in
         * {@link StandardPBEByteEncryptor}. I am extracting
         * these values to magic numbers here so when the refactoring is performed,
         * stronger decisions can be implemented here
         */
        byte[] saltBytes = cipherBytes[0..<DEFAULT_SALT_SIZE_BYTES]
        cipherBytes = cipherBytes[DEFAULT_SALT_SIZE_BYTES..-1]

        Cipher decryptionCipher = generateFlowDecryptionCipher(password, saltBytes, algorithm, provider)

        byte[] plainBytes = decryptionCipher.doFinal(cipherBytes)
        new String(plainBytes, StandardCharsets.UTF_8)
    }

    /**
     * Returns an initialized {@link javax.crypto.Cipher} instance with the extracted salt.
     *
     * @param password the password (UTF-8 encoding)
     * @param saltBytes the salt (raw bytes)
     * @param algorithm the KDF/encryption algorithm
     * @param provider the security provider
     * @return the initialized {@link javax.crypto.Cipher}
     */
    private
    static Cipher generateFlowDecryptionCipher(String password, byte[] saltBytes, String algorithm = DEFAULT_FLOW_ALGORITHM, String provider = DEFAULT_PROVIDER) {
        Cipher decryptCipher = Cipher.getInstance(algorithm, provider)
        PBEKeySpec keySpec = new PBEKeySpec(password.chars)
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(algorithm, provider)
        SecretKey pbeKey = keyFactory.generateSecret(keySpec)
        PBEParameterSpec parameterSpec = new PBEParameterSpec(saltBytes, DEFAULT_KDF_ITERATIONS)
        decryptCipher.init(Cipher.DECRYPT_MODE, pbeKey, parameterSpec)
        decryptCipher
    }

    /**
     * Encrypts a single element in the flow.xml.gz style (hex-encoded and wrapped with "enc{" and "}").
     *
     * Example:
     * "some text" -> {@code enc{0123456789ABCDEF} }
     *
     * @param plaintext the plaintext in UTF-8 encoding
     * @param saltBytes the salt to embed in the cipher text to allow key derivation and decryption later in raw format
     * @param encryptCipher the configured Cipher instance
     * @return the wrapped and hex-encoded cipher text
     */
    private static String encryptFlowElement(String plaintext, byte[] saltBytes, Cipher encryptCipher) {
        byte[] plainBytes = plaintext?.getBytes(StandardCharsets.UTF_8) ?: new byte[0]

        /* The structure of each cipher text is 16 bytes of salt || actual cipher text,
         * so extract the salt (32 bytes encoded as hex, 16 bytes raw) and combine that
         * with the default (and unchanged) iteration count that is hardcoded in
         * {@link StandardPBEByteEncryptor}. I am extracting
         * these values to magic numbers here so when the refactoring is performed,
         * stronger decisions can be implemented here
         */
        if (saltBytes.length != DEFAULT_SALT_SIZE_BYTES) {
            throw new SensitivePropertyProtectionException("The salt must be ${DEFAULT_SALT_SIZE_BYTES} bytes")
        }

        byte[] cipherBytes = encryptCipher.doFinal(plainBytes)
        byte[] saltAndCipherBytes = concatByteArrays(saltBytes, cipherBytes)

        // Encode the hex
        String hexEncodedCipherText = Hex.encodeHexString(saltAndCipherBytes)
        "enc{${hexEncodedCipherText}}"
    }

    /**
     * Utility method to quickly concatenate an arbitrary number of byte[].
     *
     * @param arrays the byte[] arrays
     * @returna single byte[] containing the values concatenated
     */
    private static byte[] concatByteArrays(byte[] ... arrays) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        arrays.each { byte[] it -> outputStream.write(it) }
        outputStream.toByteArray()
    }

    /**
     * Scans XML content and decrypts each encrypted element, then re-encrypts it with the new key, and returns the final XML content.
     *
     * @param flowXmlContent the original flow.xml.gz content
     * @param existingFlowPassword the existing value of nifi.sensitive.props.key (not a raw key, but rather a password)
     * @param newFlowPassword the password to use to for encryption (not a raw key, but rather a password)
     * @param existingAlgorithm the KDF algorithm to use (defaults to PBEWITHMD5AND256BITAES-CBC-OPENSSL)
     * @param existingProvider the {@link java.security.Provider} to use (defaults to BC)
     * @return the encrypted XML content
     */
    private String migrateFlowXmlContent(String flowXmlContent, String existingFlowPassword, String newFlowPassword, String existingAlgorithm = DEFAULT_FLOW_ALGORITHM, String existingProvider = DEFAULT_PROVIDER, String newAlgorithm = DEFAULT_FLOW_ALGORITHM, String newProvider = DEFAULT_PROVIDER) {
        /* For re-encryption, for performance reasons, we will use a fixed salt for all of
         * the operations. These values are stored in the same file and the default key is in the
         * source code (see NIFI-1465 and NIFI-1277), so the security trade-off is minimal
         * but the performance hit is substantial. We can't make this decision for
         * decryption because the FlowSerializer still uses StringEncryptor which does not
         * follow this pattern
         */
        byte[] encryptionSalt = new byte[DEFAULT_SALT_SIZE_BYTES]
        new SecureRandom().nextBytes(encryptionSalt)
        Cipher encryptCipher = generateFlowEncryptionCipher(newFlowPassword, encryptionSalt, newAlgorithm, newProvider)

        int elementCount = 0

        // Scan the XML content and identify every encrypted element, decrypt it, and replace it with the re-encrypted value
        String migratedFlowXmlContent = flowXmlContent.replaceAll(WRAPPED_FLOW_XML_CIPHER_TEXT_REGEX) { String wrappedCipherText ->
            String plaintext = decryptFlowElement(wrappedCipherText, existingFlowPassword, existingAlgorithm, existingProvider)
            byte[] cipherBytes = encryptCipher.doFinal(plaintext.bytes)
            byte[] saltAndCipherBytes = concatByteArrays(encryptionSalt, cipherBytes)
            elementCount++
            "enc{${Hex.encodeHex(saltAndCipherBytes)}}"
        }

        if (isVerbose) {
            logger.info("Decrypted and re-encrypted ${elementCount} elements for flow.xml.gz")
        }

        migratedFlowXmlContent
    }

    /**
     * Returns an initialized encryption cipher for the flow.xml.gz content.
     *
     * @param newFlowPassword the new encryption password
     * @param saltBytes the salt [16 bytes in raw format]
     * @param algorithm the KDF/encryption algorithm
     * @param provider the security provider
     * @return the initialized cipher instance
     */
    private
    static Cipher generateFlowEncryptionCipher(String newFlowPassword, byte[] saltBytes, String algorithm = DEFAULT_FLOW_ALGORITHM, String provider = DEFAULT_PROVIDER) {
        /* The Jasypt StringEncryptor implementation is final and has some design decisions
         * that will pollute this code (i.e. using a random salt on every encrypt operation
         * rather than a unique IV, so the derived key for every encrypt/decrypt operation is
         * different, which is very wasteful), so just use the standard JCE ciphers with the
         * password derived using the prescribed algorithm
         */
        Cipher encryptCipher = Cipher.getInstance(algorithm, provider)

        /* For re-encryption, for performance reasons, we will use a fixed salt for all of
         * the operations. These values are stored in the same file and the default key is in the
         * source code (see NIFI-1465 and NIFI-1277), so the security trade-off is minimal
         * but the performance hit is substantial. We can't make this decision for
         * decryption because the FlowSerializer still uses StringEncryptor which does not
         * follow this pattern
         */
        PBEKeySpec keySpec = new PBEKeySpec(newFlowPassword.chars)
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(algorithm, provider)
        SecretKey pbeKey = keyFactory.generateSecret(keySpec)
        PBEParameterSpec parameterSpec = new PBEParameterSpec(saltBytes, DEFAULT_KDF_ITERATIONS)
        encryptCipher.init(Cipher.ENCRYPT_MODE, pbeKey, parameterSpec)
        encryptCipher
    }

    /**
     * Writes the XML content to the {@link .outputFlowXmlPath} location, handling the GZIP file compression.
     *
     * @param flowXmlContent the XML content to write
     */
    private void writeFlowXmlToFile(String flowXmlContent) {
        new FileOutputStream(outputFlowXmlPath).withCloseable {
            new GZIPOutputStream(it).withCloseable {
                IOUtils.write(flowXmlContent, it, StandardCharsets.UTF_8)
            }
        }
    }

    String decryptLoginIdentityProviders(String encryptedXml, String existingKeyHex = keyHex) {
        AESSensitivePropertyProvider sensitivePropertyProvider = new AESSensitivePropertyProvider(existingKeyHex)

        try {
            def doc = new XmlSlurper().parseText(encryptedXml)
            // Find the provider element by class even if it has been renamed
            def passwords = doc.provider.find { it.'class' as String == LDAP_PROVIDER_CLASS }.property.findAll {
                it.@name =~ "Password" && it.@encryption =~ "aes/gcm/\\d{3}"
            }

            if (passwords.isEmpty()) {
                if (isVerbose) {
                    logger.info("No encrypted password property elements found in login-identity-providers.xml")
                }
                return encryptedXml
            }

            passwords.each { password ->
                if (isVerbose) {
                    logger.info("Attempting to decrypt ${password.text()}")
                }
                String decryptedValue = sensitivePropertyProvider.unprotect(password.text().trim())
                password.replaceNode {
                    property(name: password.@name, encryption: "none", decryptedValue)
                }
            }

            // Does not preserve whitespace formatting or comments
            String updatedXml = XmlUtil.serialize(doc)
            logger.info("Updated XML content: ${updatedXml}")
            updatedXml
        } catch (Exception e) {
            printUsageAndThrow("Cannot decrypt login identity providers XML content", ExitCode.SERVICE_ERROR)
        }
    }

    String decryptAuthorizers(String encryptedXml, String existingKeyHex = keyHex) {
        AESSensitivePropertyProvider sensitivePropertyProvider = new AESSensitivePropertyProvider(existingKeyHex)

        try {
            def doc = new XmlSlurper().parseText(encryptedXml)
            // Find the provider element by class even if it has been renamed
            def passwords = doc.userGroupProvider.find {
                it.'class' as String == LDAP_USER_GROUP_PROVIDER_CLASS
            }.property.findAll {
                it.@name =~ "Password" && it.@encryption =~ "aes/gcm/\\d{3}"
            }

            if (passwords.isEmpty()) {
                if (isVerbose) {
                    logger.info("No encrypted password property elements found in authorizers.xml")
                }
                return encryptedXml
            }

            passwords.each { password ->
                // TODO: Capture the raw password, and only display it in the log if the decrypted value is different (to avoid possibly printing an incorrectly provided plaintext password)
                if (isVerbose) {
                    logger.info("Attempting to decrypt ${password.text()}")
                }
                String decryptedValue = sensitivePropertyProvider.unprotect(password.text().trim())
                password.replaceNode {
                    property(name: password.@name, encryption: "none", decryptedValue)
                }
            }

            // Does not preserve whitespace formatting or comments
            String updatedXml = XmlUtil.serialize(doc)
            logger.info("Updated XML content: ${updatedXml}")
            updatedXml
        } catch (Exception e) {
            printUsageAndThrow("Cannot decrypt authorizers XML content", ExitCode.SERVICE_ERROR)
        }
    }

    String encryptLoginIdentityProviders(String plainXml, String newKeyHex = keyHex) {
        AESSensitivePropertyProvider sensitivePropertyProvider = new AESSensitivePropertyProvider(newKeyHex)

        // TODO: Switch to XmlParser & XmlNodePrinter to maintain "empty" element structure
        try {
            def doc = new XmlSlurper().parseText(plainXml)
            // Find the provider element by class even if it has been renamed
            def passwords = doc.provider.find { it.'class' as String == LDAP_PROVIDER_CLASS }
                    .property.findAll {
                // Only operate on un-encrypted passwords
                it.@name =~ "Password" && (it.@encryption == "none" || it.@encryption == "") && it.text()
            }

            if (passwords.isEmpty()) {
                if (isVerbose) {
                    logger.info("No unencrypted password property elements found in login-identity-providers.xml")
                }
                return plainXml
            }

            passwords.each { password ->
                if (isVerbose) {
                    logger.info("Attempting to encrypt ${password.name()}")
                }
                String encryptedValue = sensitivePropertyProvider.protect(password.text().trim())
                password.replaceNode {
                    property(name: password.@name, encryption: sensitivePropertyProvider.identifierKey, encryptedValue)
                }
            }

            // Does not preserve whitespace formatting or comments
            String updatedXml = XmlUtil.serialize(doc)
            logger.info("Updated XML content: ${updatedXml}")
            updatedXml
        } catch (Exception e) {
            if (isVerbose) {
                logger.error("Encountered exception", e)
            }
            printUsageAndThrow("Cannot encrypt login identity providers XML content", ExitCode.SERVICE_ERROR)
        }
    }

    String encryptAuthorizers(String plainXml, String newKeyHex = keyHex) {
        AESSensitivePropertyProvider sensitivePropertyProvider = new AESSensitivePropertyProvider(newKeyHex)

        // TODO: Switch to XmlParser & XmlNodePrinter to maintain "empty" element structure
        try {
            def doc = new XmlSlurper().parseText(plainXml)
            // Find the provider element by class even if it has been renamed
            def passwords = doc.userGroupProvider.find { it.'class' as String == LDAP_USER_GROUP_PROVIDER_CLASS }
                    .property.findAll {
                // Only operate on un-encrypted passwords
                it.@name =~ "Password" && (it.@encryption == "none" || it.@encryption == "") && it.text()
            }

            if (passwords.isEmpty()) {
                if (isVerbose) {
                    logger.info("No unencrypted password property elements found in authorizers.xml")
                }
                return plainXml
            }

            passwords.each { password ->
                if (isVerbose) {
                    logger.info("Attempting to encrypt ${password.name()}")
                }
                String encryptedValue = sensitivePropertyProvider.protect(password.text().trim())
                password.replaceNode {
                    property(name: password.@name, encryption: sensitivePropertyProvider.identifierKey, encryptedValue)
                }
            }

            // Does not preserve whitespace formatting or comments
            String updatedXml = XmlUtil.serialize(doc)
            logger.info("Updated XML content: ${updatedXml}")
            updatedXml
        } catch (Exception e) {
            if (isVerbose) {
                logger.error("Encountered exception", e)
            }
            printUsageAndThrow("Cannot encrypt authorizers XML content", ExitCode.SERVICE_ERROR)
        }
    }

    /**
     * Accepts a {@link NiFiProperties} instance, iterates over all non-empty sensitive properties which are not already marked as protected, encrypts them using the master key, and updates the property with the protected value. Additionally, adds a new sibling property {@code x.y.z.protected=aes/gcm/{128,256}} for each indicating the encryption scheme used.
     *
     * @param plainProperties the NiFiProperties instance containing the raw values
     * @return the NiFiProperties containing protected values
     */
    private NiFiProperties encryptSensitiveProperties(NiFiProperties plainProperties) {
        if (!plainProperties) {
            throw new IllegalArgumentException("Cannot encrypt empty NiFiProperties")
        }

        ProtectedNiFiProperties protectedWrapper = new ProtectedNiFiProperties(plainProperties)

        List<String> sensitivePropertyKeys = protectedWrapper.getSensitivePropertyKeys()
        if (sensitivePropertyKeys.isEmpty()) {
            logger.info("No sensitive properties to encrypt")
            return plainProperties
        }

        // Holder for encrypted properties and protection schemes
        Properties encryptedProperties = new Properties()

        AESSensitivePropertyProvider spp = new AESSensitivePropertyProvider(keyHex)
        protectedWrapper.addSensitivePropertyProvider(spp)

        List<String> keysToSkip = []

        // Iterate over each -- encrypt and add .protected if populated
        sensitivePropertyKeys.each { String key ->
            if (!plainProperties.getProperty(key)) {
                logger.debug("Skipping encryption of ${key} because it is empty")
            } else {
                String protectedValue = spp.protect(plainProperties.getProperty(key))

                // Add the encrypted value
                encryptedProperties.setProperty(key, protectedValue)
                logger.info("Protected ${key} with ${spp.getIdentifierKey()} -> \t${protectedValue}")

                // Add the protection key ("x.y.z.protected" -> "aes/gcm/{128,256}")
                String protectionKey = protectedWrapper.getProtectionKey(key)
                encryptedProperties.setProperty(protectionKey, spp.getIdentifierKey())
                logger.info("Updated protection key ${protectionKey}")

                keysToSkip << key << protectionKey
            }
        }

        // Combine the original raw NiFiProperties and the newly-encrypted properties
        // Memory-wasteful but NiFiProperties are immutable -- no setter available (unless we monkey-patch...)
        Set<String> nonSensitiveKeys = plainProperties.getPropertyKeys() - keysToSkip
        nonSensitiveKeys.each { String key ->
            encryptedProperties.setProperty(key, plainProperties.getProperty(key))
        }
        NiFiProperties mergedProperties = new StandardNiFiProperties(encryptedProperties)
        logger.info("Final result: ${mergedProperties.size()} keys including ${ProtectedNiFiProperties.countProtectedProperties(mergedProperties)} protected keys")

        mergedProperties
    }

    /**
     * Reads the existing {@code bootstrap.conf} file, updates it to contain the master key, and persists it back to the same location.
     *
     * @throw IOException if there is a problem reading or writing the bootstrap.conf file
     */
    private void writeKeyToBootstrapConf() throws IOException {
        File bootstrapConfFile
        if (bootstrapConfPath && (bootstrapConfFile = new File(bootstrapConfPath)).exists() && bootstrapConfFile.canRead() && bootstrapConfFile.canWrite()) {
            try {
                List<String> lines = bootstrapConfFile.readLines()

                updateBootstrapContentsWithKey(lines)

                // Write the updated values back to the file
                bootstrapConfFile.text = lines.join("\n")
            } catch (IOException e) {
                def msg = "Encountered an exception updating the bootstrap.conf file with the master key"
                logger.error(msg, e)
                throw e
            }
        } else {
            throw new IOException("The bootstrap.conf file at ${bootstrapConfPath} must exist and be readable and writable by the user running this tool")
        }
    }

    /**
     * Accepts the lines of the {@code bootstrap.conf} file as a {@code List <String>} and updates or adds the key property (and associated comment).
     *
     * @param lines the lines of the bootstrap file
     * @return the updated lines
     */
    private List<String> updateBootstrapContentsWithKey(List<String> lines) {
        String keyLine = "${BOOTSTRAP_KEY_PREFIX}${keyHex}"
        // Try to locate the key property line
        int keyLineIndex = lines.findIndexOf { it.startsWith(BOOTSTRAP_KEY_PREFIX) }

        // If it was found, update inline
        if (keyLineIndex != -1) {
            logger.debug("The key property was detected in bootstrap.conf")
            lines[keyLineIndex] = keyLine
            logger.debug("The bootstrap key value was updated")

            // Ensure the comment explaining the property immediately precedes it (check for edge case where key is first line)
            int keyCommentLineIndex = keyLineIndex > 0 ? keyLineIndex - 1 : 0
            if (lines[keyCommentLineIndex] != BOOTSTRAP_KEY_COMMENT) {
                lines.add(keyCommentLineIndex, BOOTSTRAP_KEY_COMMENT)
                logger.debug("A comment explaining the bootstrap key property was added")
            }
        } else {
            // If it wasn't present originally, add the comment and key property
            lines.addAll(["\n", BOOTSTRAP_KEY_COMMENT, keyLine])
            logger.debug("The key property was not detected in bootstrap.conf so it was added along with a comment explaining it")
        }

        lines
    }

    /**
     * Writes the contents of the login identity providers configuration file with encrypted values to the output {@code login-identity-providers.xml} file.
     *
     * @throw IOException if there is a problem reading or writing the login-identity-providers.xml file
     */
    private void writeLoginIdentityProviders() throws IOException {
        if (!outputLoginIdentityProvidersPath) {
            throw new IllegalArgumentException("Cannot write encrypted properties to empty login-identity-providers.xml path")
        }

        File outputLoginIdentityProvidersFile = new File(outputLoginIdentityProvidersPath)

        if (isSafeToWrite(outputLoginIdentityProvidersFile)) {
            try {
                String updatedXmlContent
                File loginIdentityProvidersFile = new File(loginIdentityProvidersPath)
                if (loginIdentityProvidersFile.exists() && loginIdentityProvidersFile.canRead()) {
                    // Instead of just writing the XML content to a file, this method attempts to maintain the structure of the original file and preserves comments
                    updatedXmlContent = serializeLoginIdentityProvidersAndPreserveFormat(loginIdentityProviders, loginIdentityProvidersFile).join("\n")
                } else {
                    updatedXmlContent = loginIdentityProviders
                }

                // Write the updated values back to the file
                outputLoginIdentityProvidersFile.text = updatedXmlContent
            } catch (IOException e) {
                def msg = "Encountered an exception updating the login-identity-providers.xml file with the encrypted values"
                logger.error(msg, e)
                throw e
            }
        } else {
            throw new IOException("The login-identity-providers.xml file at ${outputLoginIdentityProvidersPath} must be writable by the user running this tool")
        }
    }

    /**
     * Writes the contents of the authorizers configuration file with encrypted values to the output {@code authorizers.xml} file.
     *
     * @throw IOException if there is a problem reading or writing the authorizers.xml file
     */
    private void writeAuthorizers() throws IOException {
        if (!outputAuthorizersPath) {
            throw new IllegalArgumentException("Cannot write encrypted properties to empty authorizers.xml path")
        }

        File outputAuthorizersFile = new File(outputAuthorizersPath)

        if (isSafeToWrite(outputAuthorizersFile)) {
            try {
                String updatedXmlContent
                File authorizersFile = new File(authorizersPath)
                if (authorizersFile.exists() && authorizersFile.canRead()) {
                    // Instead of just writing the XML content to a file, this method attempts to maintain the structure of the original file and preserves comments
                    updatedXmlContent = serializeAuthorizersAndPreserveFormat(authorizers, authorizersFile).join("\n")
                } else {
                    updatedXmlContent = authorizers
                }

                // Write the updated values back to the file
                outputAuthorizersFile.text = updatedXmlContent
            } catch (IOException e) {
                def msg = "Encountered an exception updating the authorizers.xml file with the encrypted values"
                logger.error(msg, e)
                throw e
            }
        } else {
            throw new IOException("The authorizers.xml file at ${outputAuthorizersPath} must be writable by the user running this tool")
        }
    }

    /**
     * Writes the contents of the {@link NiFiProperties} instance with encrypted values to the output {@code nifi.properties} file.
     *
     * @throw IOException if there is a problem reading or writing the nifi.properties file
     */
    private void writeNiFiProperties() throws IOException {
        if (!outputNiFiPropertiesPath) {
            throw new IllegalArgumentException("Cannot write encrypted properties to empty nifi.properties path")
        }

        File outputNiFiPropertiesFile = new File(outputNiFiPropertiesPath)

        if (isSafeToWrite(outputNiFiPropertiesFile)) {
            try {
                List<String> linesToPersist
                File niFiPropertiesFile = new File(niFiPropertiesPath)
                if (niFiPropertiesFile.exists() && niFiPropertiesFile.canRead()) {
                    // Instead of just writing the NiFiProperties instance to a properties file, this method attempts to maintain the structure of the original file and preserves comments
                    linesToPersist = serializeNiFiPropertiesAndPreserveFormat(niFiProperties, niFiPropertiesFile)
                } else {
                    linesToPersist = serializeNiFiProperties(niFiProperties)
                }

                // Write the updated values back to the file
                outputNiFiPropertiesFile.text = linesToPersist.join("\n")
            } catch (IOException e) {
                def msg = "Encountered an exception updating the nifi.properties file with the encrypted values"
                logger.error(msg, e)
                throw e
            }
        } else {
            throw new IOException("The nifi.properties file at ${outputNiFiPropertiesPath} must be writable by the user running this tool")
        }
    }

    /**
     * Writes the contents of the secure hash configuration file (hashed value of current migration password/key hex) to the output {@code secure_hash.key} file.
     *
     * @throw IOException if there is a problem reading or writing the secure_hash.key file
     */
    private void writeSecureHash() throws IOException {
        if (!secureHashPath) {
            throw new IllegalArgumentException("Cannot write hashed password/key to empty secure_hash.key path")
        }

        File secureHashFile = new File(secureHashPath)

        if (isSafeToWrite(secureHashFile)) {
            try {
                List<String> secureHashFileLines = []
                // Calculate the secure hash of the current key (and password if provided) using current default values for cost params and random salt
                String secureHashKey = secureHashKey(keyHex)
                secureHashFileLines << "secureHashKey=${secureHashKey}"
                if (password) {
                    String secureHashPassword = secureHashPassword(password)
                    secureHashFileLines << "secureHashPassword=${secureHashPassword}"
                }

                // Write the updated values back to the file
                secureHashFile.text = secureHashFileLines.join("\n")
            } catch (IOException e) {
                def msg = "Encountered an exception updating the secure_hash.key file with the hashed value(s)"
                logger.error(msg, e)
                throw e
            }
        } else {
            throw new IOException("The secure_hash.key file at ${secureHashPath} must be writable by the user running this tool")
        }
    }

    private
    static List<String> serializeNiFiPropertiesAndPreserveFormat(NiFiProperties niFiProperties, File originalPropertiesFile) {
        List<String> lines = originalPropertiesFile.readLines()

        ProtectedNiFiProperties protectedNiFiProperties = new ProtectedNiFiProperties(niFiProperties)
        // Only need to replace the keys that have been protected AND nifi.sensitive.props.key
        Map<String, String> protectedKeys = protectedNiFiProperties.getProtectedPropertyKeys()
        if (!protectedKeys.containsKey(NiFiProperties.SENSITIVE_PROPS_KEY)) {
            protectedKeys.put(NiFiProperties.SENSITIVE_PROPS_KEY, protectedNiFiProperties.getProperty(ProtectedNiFiProperties.getProtectionKey(NiFiProperties.SENSITIVE_PROPS_KEY)))
        }

        protectedKeys.each { String key, String protectionScheme ->
            int l = lines.findIndexOf { it.startsWith(key) }
            if (l != -1) {
                lines[l] = "${key}=${protectedNiFiProperties.getProperty(key)}"
            }
            // Get the index of the following line (or cap at max)
            int p = l + 1 > lines.size() ? lines.size() : l + 1
            String protectionLine = "${protectedNiFiProperties.getProtectionKey(key)}=${protectionScheme ?: ""}"
            if (p < lines.size() && lines.get(p).startsWith("${protectedNiFiProperties.getProtectionKey(key)}=")) {
                lines.set(p, protectionLine)
            } else {
                lines.add(p, protectionLine)
            }
        }

        lines
    }

    private static List<String> serializeNiFiProperties(NiFiProperties nifiProperties) {
        OutputStream out = new ByteArrayOutputStream()
        Writer writer = new GroovyPrintWriter(out)

        // Again, waste of memory, but respecting the interface
        Properties properties = new Properties()
        nifiProperties.getPropertyKeys().each { String key ->
            properties.setProperty(key, nifiProperties.getProperty(key))
        }

        properties.store(writer, null)
        writer.flush()
        out.toString().split("\n")
    }

    static List<String> serializeLoginIdentityProvidersAndPreserveFormat(String xmlContent, File originalLoginIdentityProvidersFile) {
        // Find the provider element of the new XML in the file contents
        String fileContents = originalLoginIdentityProvidersFile.text
        try {
            def parsedXml = new XmlSlurper().parseText(xmlContent)
            def provider = parsedXml.provider.find { it.'class' as String == LDAP_PROVIDER_CLASS }
            if (provider) {
                def serializedProvider = new XmlUtil().serialize(provider)
                // Remove XML declaration from top
                serializedProvider = serializedProvider.replaceFirst(XML_DECLARATION_REGEX, "")
                fileContents = fileContents.replaceFirst(LDAP_PROVIDER_REGEX, serializedProvider)
                return fileContents.split("\n")
            } else {
                throw new SAXException("No ldap-provider element found")
            }
        } catch (SAXException e) {
            logger.error("No provider element with class {} found in XML content; " +
                    "the file could be empty or the element may be missing or commented out", LDAP_PROVIDER_CLASS)
            return fileContents.split("\n")
        }
    }

    static List<String> serializeAuthorizersAndPreserveFormat(String xmlContent, File originalAuthorizersFile) {
        // Find the provider element of the new XML in the file contents
        String fileContents = originalAuthorizersFile.text
        try {
            def parsedXml = new XmlSlurper().parseText(xmlContent)
            def provider = parsedXml.userGroupProvider.find { it.'class' as String == LDAP_USER_GROUP_PROVIDER_CLASS }
            if (provider) {
                def serializedProvider = new XmlUtil().serialize(provider)
                // Remove XML declaration from top
                serializedProvider = serializedProvider.replaceFirst(XML_DECLARATION_REGEX, "")
                fileContents = fileContents.replaceFirst(LDAP_USER_GROUP_PROVIDER_REGEX, serializedProvider)
                return fileContents.split("\n")
            } else {
                throw new SAXException("No ldap-user-group-provider element found")
            }
        } catch (SAXException e) {
            logger.error("No provider element with class {} found in XML content; " +
                    "the file could be empty or the element may be missing or commented out", LDAP_USER_GROUP_PROVIDER_CLASS)
            return fileContents.split("\n")
        }
    }

    /**
     * Helper method which returns true if it is "safe" to write to the provided file.
     *
     * Conditions:
     *  file does not exist and the parent directory is writable
     *  -OR-
     *  file exists and is writable
     *
     * @param fileToWrite the proposed file to be written to
     * @return true if the caller can "safely" write to this file location
     */
    private static boolean isSafeToWrite(File fileToWrite) {
        fileToWrite && ((!fileToWrite.exists() && fileToWrite.absoluteFile.parentFile.canWrite()) || (fileToWrite.exists() && fileToWrite.canWrite()))
    }

    private static String deriveKeyFromPassword(String password) {
        password = password?.trim()
        if (!password || password.length() < MIN_PASSWORD_LENGTH) {
            throw new KeyException("Cannot derive key from empty/short password -- password must be at least ${MIN_PASSWORD_LENGTH} characters")
        }

        // Generate a 128 bit salt
        byte[] salt = generateScryptSaltForKeyDerivation()
        int keyLengthInBytes = getValidKeyLengths().max() / 8
        byte[] derivedKeyBytes = SCrypt.generate(password.getBytes(StandardCharsets.UTF_8), salt, SCRYPT_N, SCRYPT_R, SCRYPT_P, keyLengthInBytes)
        Hex.encodeHexString(derivedKeyBytes).toUpperCase()
    }

    /**
     * Returns a static "raw" salt (the 128 bits of random data used when generating the hash, not the "complete" {@code $s0$e0101$ABCDEFGHIJKLMNOPQRSTUV} salt format).
     * @return the raw salt in byte[] form
     */
    private static byte[] generateScryptSaltForKeyDerivation() {
//        byte[] salt = new byte[16]
//        new SecureRandom().nextBytes(salt)
//        salt
        /* It is not ideal to use a static salt, but the KDF operation must be deterministic
        for a given password, and storing and retrieving the salt in bootstrap.conf causes
        compatibility concerns
        */
        "NIFI_SCRYPT_SALT".getBytes(StandardCharsets.UTF_8)
    }

    private static byte[] generateRandomSalt(int bits = 128) {
        byte[] salt = new byte[bits / 8]
        new SecureRandom().nextBytes(salt)
        salt
    }

    /**
     * Generates an scrypt salt using the provided parameters and encodes it in the proper format. If a value is not provided, the listed default will be used.
     *
     * @param rawSalt the raw 128 bit salt to use (default is to generate a random value)
     * @param n the CPU/memory cost factor (default is {@value ConfigEncryptionTool#SCRYPT_N})
     * @param r the blocksize factor (default is {@value ConfigEncryptionTool#SCRYPT_R})
     * @param p the parallelization factor (default is {@value ConfigEncryptionTool#SCRYPT_P})
     * @param version the salt version identifier (default is {@value ConfigEncryptionTool#CURRENT_SCRYPT_VERSION})
     * @return the scrypt-formatted salt (e.g. {@code $s0$e0101$ABCDEFGHIJKLMNOPQRSTUV})
     */
    private
    static String generateScryptSalt(byte[] rawSalt = generateRandomSalt(128), int n = SCRYPT_N, int r = SCRYPT_R, int p = SCRYPT_P, String version = CURRENT_SCRYPT_VERSION) {
        String formattedSalt = Scrypt.formatSalt(rawSalt, n, r, p)
        def versionString = "\$${version}\$"
        if (!formattedSalt.startsWith(versionString)) {
            formattedSalt = formattedSalt.replaceFirst(/$\w{2}$/, versionString)
        }
        return formattedSalt
    }

    private String getExistingFlowPassword() {
        return niFiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY) as String ?: DEFAULT_NIFI_SENSITIVE_PROPS_KEY
    }

    /**
     * Utility method which returns true if the {@link org.apache.nifi.util.NiFiProperties} instance has encrypted properties.
     *
     * @return true if the properties instance will require a key to access
     */
    boolean niFiPropertiesAreEncrypted() {
        if (niFiPropertiesPath) {
            try {
                def nfp = NiFiPropertiesLoader.withKey(keyHex).readProtectedPropertiesFromDisk(new File(niFiPropertiesPath))
                return nfp.hasProtectedKeys()
            } catch (SensitivePropertyProtectionException | IOException e) {
                return true
            }
        } else {
            return false
        }
    }

    static String secureHashPassword(String password, String salt = generateScryptSalt()) {
        // If empty, generate complete salt
        if (!salt) {
            salt = generateScryptSalt()
        }

        // The findAll() drops any empty elements
        def saltComponents = salt.split('\\$').findAll()
        if (saltComponents.size() < 3) {
            throw new IllegalArgumentException("The provided salt was not in valid scrypt format")
        }
        if (saltComponents.first() != CURRENT_SCRYPT_VERSION) {
            throw new IllegalArgumentException("Currently, only scrypt hashes with version ${CURRENT_SCRYPT_VERSION} are supported")
        }

        // Split the encoded params into N, R, P
        List<Integer> params = Scrypt.parseParameters(saltComponents[1])

        // Generate the hashed format
        Scrypt.scrypt(password, Base64.decoder.decode(saltComponents[2]), params[0], params[1], params[2], AMBARI_COMPATIBLE_SCRYPT_HASH_LENGTH)
    }

    static String secureHashKey(String keyHex, String salt = generateScryptSalt()) {
        // Lowercase the key hex, then call secureHashPassword() as the algorithm is the same
        secureHashPassword(keyHex?.toLowerCase(), salt)
    }


    private String getCurrentHashParams() {
        try {
            int N
            int r
            int p
            String base64Salt

            try {
                String secureHash = readSecureHashValueFromFile()

                // The findAll() drops any empty elements
                def hashComponents = secureHash.split('\\$').findAll()
                if (hashComponents.size() < 3) {
                    throw new IllegalArgumentException("The provided secure hash was not in valid scrypt format")
                }
                if (hashComponents.first() != CURRENT_SCRYPT_VERSION) {
                    throw new IllegalArgumentException("Currently, only scrypt hashes with version ${CURRENT_SCRYPT_VERSION} are supported")
                }

                // Split the encoded params into N, R, P
                List<Integer> params = Scrypt.parseParameters(hashComponents[1])
                N = params[0]
                r = params[1]
                p = params[2]
                base64Salt = hashComponents[2]
            } catch (IOException | InvalidKeyException e) {
                // These exceptions occur if the file doesn't exist, can't be read, or doesn't have secure hashes populated
                N = SCRYPT_N
                r = SCRYPT_R
                p = SCRYPT_P
                base64Salt = CipherUtility.encodeBase64NoPadding(generateRandomSalt())
            }
            return new JsonBuilder([N: N, r: r, p: p, salt: base64Salt]).toString()
        } catch (Exception e) {
            logger.error("Encountered an exception getting current hash parameters: ${e.getLocalizedMessage()}")
            printUsageAndThrow(e.getLocalizedMessage(), ExitCode.ERROR_READING_CONFIG)
        }
    }

    /**
     * Runs main tool logic (parsing arguments, reading files, protecting properties, and writing key and properties out to destination files).
     *
     * @param args the command-line arguments
     */
    static void main(String[] args) {
        Security.addProvider(new BouncyCastleProvider())

        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        try {
            try {
                tool.parse(args)

                // Ensure the only content written to STDOUT is the JSON (consumable by other processes)
                if (tool.queryingCurrentHashParams) {
                    System.out.println(tool.getCurrentHashParams())
                    System.exit(ExitCode.SUCCESS.ordinal())
                }

                boolean existingNiFiPropertiesAreEncrypted = tool.niFiPropertiesAreEncrypted()
                if (!tool.ignorePropertiesFiles || (tool.handlingFlowXml && existingNiFiPropertiesAreEncrypted)) {
                    // If we are handling the flow.xml.gz and nifi.properties is already encrypted, try getting the key from bootstrap.conf rather than the console
                    if (tool.ignorePropertiesFiles) {
                        tool.keyHex = NiFiPropertiesLoader.extractKeyFromBootstrapFile(tool.bootstrapConfPath)
                    } else {
                        tool.keyHex = tool.getKey()
                    }

                    if (!tool.keyHex) {
                        tool.printUsageAndThrow("Hex key must be provided", ExitCode.INVALID_ARGS)
                    }

                    try {
                        // Validate the length and format
                        tool.keyHex = parseKey(tool.keyHex)
                    } catch (KeyException e) {
                        if (tool.isVerbose) {
                            logger.error("Encountered an error", e)
                        }
                        tool.printUsageAndThrow(e.getMessage(), ExitCode.INVALID_ARGS)
                    }

                    if (tool.migration) {
                        String migrationKeyHex = tool.getMigrationKey()

                        if (!migrationKeyHex) {
                            tool.printUsageAndThrow("Original hex key must be provided for migration", ExitCode.INVALID_ARGS)
                        }

                        try {
                            // Validate the length and format
                            tool.migrationKeyHex = parseKey(migrationKeyHex)
                        } catch (KeyException e) {
                            if (tool.isVerbose) {
                                logger.error("Encountered an error", e)
                            }
                            tool.printUsageAndThrow(e.getMessage(), ExitCode.INVALID_ARGS)
                        }
                    }
                }
                String existingKeyHex = tool.migrationKeyHex ?: tool.keyHex

                // Load NiFiProperties for either scenario; only encrypt if "handling" (see after flow XML)
                if (tool.handlingNiFiProperties || tool.handlingFlowXml) {
                    try {
                        tool.niFiProperties = tool.loadNiFiProperties(existingKeyHex)
                    } catch (Exception e) {
                        tool.printUsageAndThrow("Cannot migrate key if no previous encryption occurred", ExitCode.ERROR_READING_NIFI_PROPERTIES)
                    }
                }

                if (tool.handlingLoginIdentityProviders) {
                    try {
                        tool.loginIdentityProviders = tool.loadLoginIdentityProviders(existingKeyHex)
                    } catch (Exception e) {
                        tool.printUsageAndThrow("Cannot migrate key if no previous encryption occurred", ExitCode.ERROR_INCORRECT_NUMBER_OF_PASSWORDS)
                    }
                    tool.loginIdentityProviders = tool.encryptLoginIdentityProviders(tool.loginIdentityProviders)
                }

                if (tool.handlingAuthorizers) {
                    try {
                        tool.authorizers = tool.loadAuthorizers(existingKeyHex)
                    } catch (Exception e) {
                        tool.printUsageAndThrow("Cannot migrate key if no previous encryption occurred", ExitCode.ERROR_INCORRECT_NUMBER_OF_PASSWORDS)
                    }
                    tool.authorizers = tool.encryptAuthorizers(tool.authorizers)
                }

                if (tool.handlingFlowXml) {
                    try {
                        tool.flowXml = tool.loadFlowXml()
                    } catch (Exception e) {
                        tool.printUsageAndThrow("Cannot load flow.xml.gz", ExitCode.ERROR_READING_NIFI_PROPERTIES)
                    }

                    // If the flow password was not set in nifi.properties, use the hard-coded default
                    String existingFlowPassword = tool.getExistingFlowPassword()

                    // If the new password was not provided in the arguments, read from the console. If that is empty, use the same value (essentially a copy no-op)
                    String newFlowPassword = tool.flowPropertiesPassword ?: tool.getFlowPassword()
                    if (!newFlowPassword) {
                        newFlowPassword = existingFlowPassword
                    }

                    // Get the algorithms and providers
                    NiFiProperties nfp = tool.niFiProperties
                    String existingAlgorithm = nfp?.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM) ?: DEFAULT_FLOW_ALGORITHM
                    String existingProvider = nfp?.getProperty(NiFiProperties.SENSITIVE_PROPS_PROVIDER) ?: DEFAULT_PROVIDER

                    String newAlgorithm = tool.newFlowAlgorithm ?: existingAlgorithm
                    String newProvider = tool.newFlowProvider ?: existingProvider

                    tool.flowXml = tool.migrateFlowXmlContent(tool.flowXml, existingFlowPassword, newFlowPassword, existingAlgorithm, existingProvider, newAlgorithm, newProvider)

                    // If the new key is the hard-coded internal value, don't persist it to nifi.properties
                    if (newFlowPassword != DEFAULT_NIFI_SENSITIVE_PROPS_KEY && newFlowPassword != existingFlowPassword) {
                        // Update the NiFiProperties object with the new flow password before it gets encrypted (wasteful, but NiFiProperties instances are immutable)
                        Properties rawProperties = new Properties()
                        nfp.getPropertyKeys().each { String k ->
                            rawProperties.put(k, nfp.getProperty(k))
                        }

                        // If the tool is not going to encrypt NiFiProperties and the existing file is already encrypted, encrypt and update the new sensitive props key
                        if (!tool.handlingNiFiProperties && existingNiFiPropertiesAreEncrypted) {
                            AESSensitivePropertyProvider spp = new AESSensitivePropertyProvider(tool.keyHex)
                            String encryptedSPK = spp.protect(newFlowPassword)
                            rawProperties.put(NiFiProperties.SENSITIVE_PROPS_KEY, encryptedSPK)
                            // Manually update the protection scheme or it will be lost
                            rawProperties.put(ProtectedNiFiProperties.getProtectionKey(NiFiProperties.SENSITIVE_PROPS_KEY), spp.getIdentifierKey())
                            if (tool.isVerbose) {
                                logger.info("Tool is not configured to encrypt nifi.properties, but the existing nifi.properties is encrypted and flow.xml.gz was migrated, so manually persisting the new encrypted value to nifi.properties")
                            }
                        } else {
                            rawProperties.put(NiFiProperties.SENSITIVE_PROPS_KEY, newFlowPassword)
                        }
                        tool.niFiProperties = new StandardNiFiProperties(rawProperties)
                    }
                }

                if (tool.handlingNiFiProperties) {
                    tool.niFiProperties = tool.encryptSensitiveProperties(tool.niFiProperties)
                }
            } catch (CommandLineParseException e) {
                if (e.exitCode == ExitCode.HELP) {
                    System.exit(ExitCode.HELP.ordinal())
                }
                throw e
            } catch (Exception e) {
                if (tool.isVerbose) {
                    logger.error("Encountered an error", e)
                }
                tool.printUsageAndThrow(e.message, ExitCode.ERROR_PARSING_COMMAND_LINE)
            }

            try {
                // Do this as part of a transaction?
                synchronized (this) {
                    if (!tool.ignorePropertiesFiles) {
                        tool.writeKeyToBootstrapConf()

                        // Always write the secure hash in case the next invocation needs it
                        tool.writeSecureHash()
                    }
                    if (tool.handlingFlowXml) {
                        tool.writeFlowXmlToFile(tool.flowXml)
                    }
                    if (tool.handlingNiFiProperties || tool.handlingFlowXml) {
                        tool.writeNiFiProperties()
                    }
                    if (tool.handlingLoginIdentityProviders) {
                        tool.writeLoginIdentityProviders()
                    }
                    if (tool.handlingAuthorizers) {
                        tool.writeAuthorizers()
                    }
                }
            } catch (Exception e) {
                if (tool.isVerbose) {
                    logger.error("Encountered an error", e)
                }
                tool.printUsageAndThrow("Encountered an error writing the master key to the bootstrap.conf file and the encrypted properties to nifi.properties", ExitCode.ERROR_GENERATING_CONFIG)
            }
        } catch (CommandLineParseException e) {
            System.exit(e.exitCode.ordinal())
        }

        System.exit(ExitCode.SUCCESS.ordinal())
    }
}
