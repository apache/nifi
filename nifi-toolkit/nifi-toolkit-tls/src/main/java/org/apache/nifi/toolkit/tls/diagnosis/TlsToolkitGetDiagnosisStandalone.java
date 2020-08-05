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

package org.apache.nifi.toolkit.tls.diagnosis;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.conn.util.InetAddressUtils;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.security.kms.CryptoUtils;
import org.apache.nifi.security.util.CertificateUtils;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.commandLine.ExitCode;
import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.apache.nifi.util.NiFiProperties;

import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.Tuple;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PublicKey;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.security.interfaces.DSAPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class TlsToolkitGetDiagnosisStandalone {

    private static final String NIFI_PROPERTIES_ARG = "nifiProperties";
    private static final String HELP_ARG = "help";
    private static final String QUIET_ARG = "quiet";
    private static final String BOOTSTRAP_ARG = "bootstrap";
    private static final String CN = "CN";
    private static final String SAN = "SAN";
    private static final String EKU = "EKU";
    private static final String VALIDITY = "VALIDITY";
    private static final String KEYSIZE = "KEYSIZE";
    private static final String SIGN = "SIGN";
    private static final String TRUSTSTORE = "TRUSTSTORE";
    private final Options options;

    private String keystorePath;
    private String keystoreType;
    private KeyStore keystore;

    private String truststorePath;
    private String truststoreType;
    private KeyStore truststore;

    private String niFiPropertiesPath;
    private String bootstrapPath;
    private NiFiProperties niFiProperties;

    private static Map<String, String> createEKUMap() {
        Map<String, String> orderMap = new HashMap<>();
        int count = 0;
        orderMap.put("serverAuth", "1.3.6.1.5.5.7.3.1");
        orderMap.put("clientAuth", "1.3.6.1.5.5.7.3.2");
        return Collections.unmodifiableMap(orderMap);
    }

    private static Map<String, String> ekuMap = createEKUMap();

    enum Output {
        CORRECT,
        WRONG,
        NEEDS_ATTENTION
    }

    private static Map<String, Tuple<String, Output>> outputSummary = new LinkedHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(TlsToolkitGetDiagnosisStandalone.class);

    public TlsToolkitGetDiagnosisStandalone() {
        this.options = buildOptions();
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(Option.builder("n").longOpt(NIFI_PROPERTIES_ARG).hasArg(true).argName("file").desc("This field specifies nifi.properties file name").build());
        options.addOption(Option.builder("h").longOpt(HELP_ARG).hasArg(false).desc("Show usage information (this message)").build());
        options.addOption(Option.builder("q").longOpt(QUIET_ARG).hasArg(false).desc("Suppresses log info messages").build());
        options.addOption(Option.builder("b").longOpt(BOOTSTRAP_ARG).hasArg(true).desc("Suppresses log info messages").build());
        return options;
    }

    private void parseCommandLine(String[] args) throws CommandLineParseException {
        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption(HELP_ARG)) {
                printUsage("");
                System.exit(0);
            }
            //nifi.properties present?
            if (commandLine.hasOption(NIFI_PROPERTIES_ARG)) {
                niFiPropertiesPath = commandLine.getOptionValue(NIFI_PROPERTIES_ARG);
                logger.info("Parsed nifi.properties path: " + niFiPropertiesPath);

                if (commandLine.hasOption(BOOTSTRAP_ARG)) {
                    bootstrapPath = commandLine.getOptionValue(BOOTSTRAP_ARG);
                } else {
                    logger.info("No bootstrap.conf provided. Looking in nifi.properties directory");
                    bootstrapPath = new File(niFiPropertiesPath).getParent() + "/bootstrap.conf";
                }

                logger.info("Parsed bootstrap.conf path: " + bootstrapPath);
            }

        } catch (ParseException e) {
            logger.error("Encountered an error while parsing command line");
            printAndThrowParsingException("Error parsing command line. (" + e.getMessage() + ")", ExitCode.ERROR_PARSING_COMMAND_LINE);
        }
    }

    public static void printUsage(String errorMessage) {
        if (!errorMessage.isEmpty()) {
            System.out.println(errorMessage);
            System.out.println();
        }
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(160);
        helpFormatter.setOptionComparator(null);
        // preserve manual ordering of options when printing instead of alphabetical
        helpFormatter.printHelp(TlsToolkitGetDiagnosisStandalone.class.getCanonicalName(), buildOptions(), true);
    }

    public static void printAndThrowParsingException(String errorMessage, ExitCode exitCode) throws CommandLineParseException {
        printUsage(errorMessage);
        throw new CommandLineParseException(errorMessage, exitCode);
    }

    private static void displaySummaryReport() {
        int correct = 0, wrong = 0, needsAttention = 0;
        System.out.println("\n***********STANDALONE DIAGNOSIS SUMMARY***********\n");
        for (Map.Entry<String, Tuple<String, Output>> each : outputSummary.entrySet()) {
            String output = each.getValue().getValue().toString();
            String type = StringUtils.rightPad(each.getKey(), 12);
            System.out.println(type + " ==>   " + each.getValue().getKey());
            switch (output) {
                case "WRONG":
                    wrong++;
                    break;
                case "CORRECT":
                    correct++;
                    break;
                case "NEEDS_ATTENTION":
                    needsAttention++;
                    break;
            }
        }
        System.out.println("\nCORRECT checks:         " + correct + "/7");
        System.out.println("WRONG checks:           " + wrong + "/7");
        System.out.println("NEEDS ATTENTION checks: " + needsAttention + "/7");
        System.out.println("**************************************************\n");
    }


    public static void main(String[] args) {
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone();

        // TODO: If -v was added, change the logging config value

        //Parse
        try {
            standalone.parseCommandLine(args);
            standalone.niFiProperties = standalone.loadNiFiProperties();
        } catch (CommandLineParseException e) {
            System.exit(e.getExitCode().ordinal());
        } catch (IOException e) {
            printUsage(e.getLocalizedMessage());
            System.exit(-1);
        }

        //Get keystore and truststore path
        standalone.keystorePath = standalone.niFiProperties.getProperty("nifi.security.keystore");
        standalone.truststorePath = standalone.niFiProperties.getProperty("nifi.security.truststore");
        char[] keystorePassword = standalone.niFiProperties.getProperty("nifi.security.keystorePasswd").toCharArray();
        standalone.keystoreType = standalone.niFiProperties.getProperty("nifi.security.keystoreType");
        standalone.truststoreType = standalone.niFiProperties.getProperty("nifi.security.truststoreType");
        char[] truststorePassword = standalone.niFiProperties.getProperty("nifi.security.truststorePasswd").toCharArray();

        //Verify keystore and truststore are located at the correct file path
        if ((doesFileExist(standalone.keystorePath, standalone.niFiPropertiesPath, ".jks")
                && doesFileExist(standalone.truststorePath, standalone.niFiPropertiesPath, ".jks"))) {

            //check keystore and truststore password
            standalone.keystore = checkPasswordForKeystoreAndLoadKeystore(keystorePassword, standalone.keystorePath, standalone.keystoreType);
            standalone.truststore = checkPasswordForKeystoreAndLoadKeystore(truststorePassword, standalone.truststorePath, standalone.truststoreType);
            if (!(standalone.keystore == null) && !(standalone.truststore == null)) {
                // TODO: Refactor "dangerous" logic to method which throws exceptions
                KeyStore.PrivateKeyEntry privateKeyEntry = standalone.extractPrimaryPrivateKeyEntry(standalone.keystore, keystorePassword);
                if (privateKeyEntry != null) {
                    if (standalone.identifyHostUsingKeystore(privateKeyEntry)) {
                        outputSummary.put(TRUSTSTORE, standalone.checkTruststore(privateKeyEntry));

                        displaySummaryReport();
                    } else {
                        System.exit(-1);
                    }
                } else {
                    System.exit(-1);
                }
            } else {
                System.exit(-1);
            }
        } else {
            System.exit(-1);
        }
    }

    private KeyStore.PrivateKeyEntry extractPrimaryPrivateKeyEntry(KeyStore keystore, char[] keystorePassword) {
        try {
            KeyStore.PasswordProtection keystorePasswordProtection = new KeyStore.PasswordProtection(keystorePassword);
            List<String> keystoreAliases = Collections.list(keystore.aliases());
            Map<String, KeyStore.Entry> privateEntries = keystoreAliases.stream()
                    .map(alias -> retrieveEntryFromKeystore(keystorePasswordProtection, alias))
                    .filter(Objects::nonNull)
                    .filter(t -> t.getValue() instanceof KeyStore.PrivateKeyEntry)
                    .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));

            //Check # of privateKeyEntry(s)
            if (privateEntries.size() == 0) {
                logger.error("No privateKeyEntry in keystore. Cannot explore keystore identification.");
                return null;
            } else if (privateEntries.size() > 1) {
                logger.info("Keystore has multiple privateKeyEntries. Using the first privateKeyEntry in the list: " + new ArrayList<>(privateEntries.keySet()).get(0));
                logger.warn("Recommended to have a single PrivateKeyEntry in keystore");
                logger.warn("Available PrivateKeyEntries: " + StringUtils.join(privateEntries.keySet(), ", "));
            } else {
                logger.info("Keystore has single privateKeyEntry: " + new ArrayList<>(privateEntries.keySet()).get(0));
            }
            return ((KeyStore.PrivateKeyEntry) new ArrayList<>(privateEntries.values()).get(0));
        } catch (KeyStoreException e) {
            logger.error("Something went wrong: " + e.getLocalizedMessage());
            return null;
        }
    }

    private boolean identifyHostUsingKeystore(KeyStore.PrivateKeyEntry privateKeyEntry) {

        X509Certificate x509Certificate = (X509Certificate) privateKeyEntry.getCertificate();

        if (x509Certificate != null) {
            String specifiedHostname = niFiProperties.getProperty("nifi.web.https.host");
            if (specifiedHostname.contains("*.")) {
                logger.error("Hostname in nifi.properties file is a WILDCARD: Cannot proceed with diagnosis");
                return false;
            }
            // [1] CN
            outputSummary.put(CN, checkCN(x509Certificate, specifiedHostname));
            // [2] SAN
            outputSummary.put(SAN, checkSAN(x509Certificate, specifiedHostname));
            //[3] EKU
            outputSummary.put(EKU, checkEKU(x509Certificate));
            //[4] Validity dates
            outputSummary.put(VALIDITY, checkValidity(x509Certificate));
            //[5] Key size
            outputSummary.put(KEYSIZE, checkKeySize(x509Certificate));
            //[6] Signature
            List<X509Certificate> certificateList = Arrays.stream(((X509Certificate[]) privateKeyEntry.getCertificateChain())).sequential().collect(Collectors.toList());
            outputSummary.put(SIGN, checkSignature(certificateList, x509Certificate));
            return true;
        } else {
            logger.error("Error loading X509 certificate: Check privateKeyEntry of keystore");
            return false;
        }
    }

    private Tuple<String, Output> checkTruststore(KeyStore.PrivateKeyEntry privateKeyEntry) {

        String number = "[7] ";
        try {
            List<String> truststoreAliases = Collections.list(truststore.aliases());
            List<X509Certificate> trustedCertificateEntries = truststoreAliases.stream().map(this::getTrustedCertificates).collect(Collectors.toList());

            X509Certificate privateKeyEntryCert = (X509Certificate) privateKeyEntry.getCertificate();

            if (TlsHelper.verifyCertificateSignature(privateKeyEntryCert, trustedCertificateEntries)) {
                logger.info(number + "truststore contains a public certificate identifying privateKeyEntry in keystore\n");
                return new Tuple<>(number + "Truststore identifies privateKeyEntry in keystore", Output.CORRECT);
            } else {
                logger.error(number + "truststore does not contain a public certificate identifying privateKeyEntry in keystore\n");
                return new Tuple<>(number + "Truststore does not identify privateKeyEntry in keystore", Output.WRONG);
            }
        } catch (KeyStoreException e) {
            logger.error(number + e.getLocalizedMessage());
            return new Tuple<>("[7] " + e.getLocalizedMessage(), Output.NEEDS_ATTENTION);
        }
    }

    private X509Certificate getTrustedCertificates(String alias) {
        try {
            return (X509Certificate) truststore.getCertificate(alias);
        } catch (KeyStoreException e) {
            logger.error(e.getLocalizedMessage());
        }
        return null;
    }

    private static Tuple<String, Output> checkCN(X509Certificate x509Certificate, String specifiedHostname) {

        X500Name x500Name = new X500Name(x509Certificate.getSubjectX500Principal().getName());
        String subjectCN = CertificateUtils.extractUsername(x500Name.toString());

        if (subjectCN.contains("*.")) {
            logger.info("[1] CN: Subject CN = " + subjectCN + " is a wildcard\n");
            logger.info("    Check SAN entry for '" + specifiedHostname + "'");
            logger.warn("    Wildcard certificates are not recommended nor supported for NiFi");
            return new Tuple<>("[1] CN is wildcard. Check SAN", Output.NEEDS_ATTENTION);
        } else if (subjectCN.equals(specifiedHostname)) {
            //Exact match
            logger.info("[1] CN: Subject CN = " + subjectCN + " matches with host in nifi.properties\n");
            return new Tuple<>("[1] CN is CORRECT", Output.CORRECT);
        } else {
            logger.error("[1] Subject CN = " + subjectCN + " doesn't match with hostname in nifi.properties file");
            logger.error("    Check nifi.web.https.host value.");
            logger.error("    Current nifi.web.https.host = " + specifiedHostname + "\n");
            return new Tuple<>("[1] CN is different than hostname. Compare CN with nifi.web.https.host in nifi.properties", Output.WRONG);
        }
    }

    private static Tuple<String, Output> checkSAN(X509Certificate x509Certificate, String specifiedHostname) {

        boolean specifiedHostnameIsIP = false;

        //Check if specified hostname is IP
        if (InetAddressUtils.isIPv4Address(specifiedHostname) || InetAddressUtils.isIPv6Address(specifiedHostname)) {
            specifiedHostnameIsIP = true;
        }

        //Get all SANs
        Map<String, String> sanMap = null;
        try {
            sanMap = CertificateUtils.getSubjectAlternativeNamesMap(x509Certificate);
        } catch (CertificateParsingException e) {
            logger.error("Error in SAN check: " + e.getLocalizedMessage());
            return new Tuple<>("[2] SAN: Error in SAN check: " + e.getLocalizedMessage(), Output.NEEDS_ATTENTION);
        }

        //Check and load IP or DNS SAN entries
        List<String> sanListDNS;
        List<String> sanListIP;
        if (sanMap.containsValue(("dNSName")) || sanMap.containsValue(("iPAddress"))) {
            sanListDNS = sanMap.entrySet().stream().filter(t -> "dNSName".equals(t.getValue())).map(Map.Entry::getKey).collect(Collectors.toList());
            sanListIP = sanMap.entrySet().stream().filter(t -> "iPAddress".equals(t.getValue())).map(Map.Entry::getKey).collect(Collectors.toList());
        } else {
            logger.error("[2] No DNS or IPAddress entry present in SAN");
            return new Tuple<>("[2] SAN is empty. ==> Add a SAN entry matching " + specifiedHostname, Output.WRONG);
        }

        //specifiedHostname is a domain name
        if (!specifiedHostnameIsIP) {

            //SAN has the specified domain name
            if (sanListDNS.size() != 0 && sanListDNS.contains(specifiedHostname)) {
                logger.info("[2] SAN: DNS = " + specifiedHostname + " in SAN matches with host in nifi.properties\n");
                return new Tuple<>("[2] SAN entry represents " + specifiedHostname, Output.CORRECT);
            } else {
                if (sanListDNS.size() == 0) {
                    logger.warn("[2] SAN: SAN doesn't have DNS entry. Checking IP entries.");
                } else {
                    logger.warn("[2] SAN: SAN DNS entry doesn't match with host '" + specifiedHostname + "' in nifi.properties. Checking IP entries.");
                }
                //check for IP entries in SAN to match with resolved specified hostname
                if (sanListIP.size() != 0) {
                    try {
                        String ipAddress = InetAddress.getByName(specifiedHostname).getHostAddress();
                        if (sanListIP.contains(ipAddress)) {
                            logger.info("    SAN: IP = " + ipAddress + " in SAN  matches with host in nifi.properties after resolution\n");
                            return new Tuple<>("[2] SAN entry represents " + specifiedHostname, Output.CORRECT);
                        } else {
                            logger.error("    No IP address entries found in SAN that represent " + specifiedHostname);
                            logger.error("    Add DNS/IP entry in SAN for hostname: " + specifiedHostname + "\n");
                            return new Tuple<>("[2] SAN entries do not represent hostname in nifi.properties. Add DNS/IP entry in SAN for hostname: " + specifiedHostname, Output.WRONG);
                        }
                    } catch (UnknownHostException e) {
                        logger.error("    " + e.getLocalizedMessage() + "\n");
                        return new Tuple<>("[2] Unable to resolve hostname in nifi.properties to IP ", Output.NEEDS_ATTENTION);
                    }

                } else {
                    //No IP entries present in SAN
                    logger.error("    No IP address entries found in SAN to resolve.");
                    logger.error("    Add DNS/IP entry in SAN for hostname: " + specifiedHostname + "\n");
                    return new Tuple<>("[2] SAN entries do not represent hostname in nifi.properties. Add DNS/IP entry in SAN for hostname: " + specifiedHostname, Output.WRONG);
                }
            }
        } else { //nifi.web.https.host is an IP address
            if (sanListIP.size() != 0 && sanListIP.contains(specifiedHostname)) {
                logger.info("[2] SAN: IP = " + specifiedHostname + " in SAN matches with host in nifi.properties\n");
                return new Tuple<>("[2] SAN entry represents " + specifiedHostname, Output.CORRECT);
            } else {
                if (sanListIP.size() == 0) {
                    logger.error("[2] SAN: SAN doesn't have IP entry");
                    logger.error("    Add IP entry in SAN for host IP: " + specifiedHostname + "\n");
                    return new Tuple<>("[2] SAN has no IP entries. Add IP entry in SAN for hostname: " + specifiedHostname, Output.WRONG);
                } else {
                    return new Tuple<>("[2] SAN IP entries do not represent hostname in nifi.properties. Add IP entry in SAN for hostname: " + specifiedHostname, Output.WRONG);
                }
            }
        }
    }

    private static Tuple<String, Output> checkEKU(X509Certificate x509Certificate) {
        List<String> eKU = null;
        try {
            eKU = x509Certificate.getExtendedKeyUsage();
        } catch (CertificateParsingException e) {
            logger.error("Error in EKU check: " + e.getLocalizedMessage());
            return new Tuple<>("Error in EKU check: " + e.getLocalizedMessage(), Output.WRONG);
        }
        if (eKU != null) {
            if (!eKU.contains(ekuMap.get("serverAuth")) && !eKU.contains(ekuMap.get("clientAuth"))) {
                logger.error("[3] EKU: serverAuth and clientAuth absent");
                logger.error("    Add serverAuth and clientAuth to the EKU of the certificate\n");
                return new Tuple<>("[3] EKUs serverAuth and clientAuth needs to be added to the certificate.", Output.WRONG);
            }

            if (eKU.contains(ekuMap.get("serverAuth")) && eKU.contains(ekuMap.get("clientAuth"))) {
                logger.info("[3] EKU: serverAuth and clientAuth present\n");
                return new Tuple<>("[3] EKUs are correct. ", Output.CORRECT);
            } else if (!eKU.contains(ekuMap.get("serverAuth"))) {
                logger.error("[3] EKU: serverAuth is absent");
                logger.error("    Add serverAuth to the EKU of the certificate\n");
                return new Tuple<>("[3] EKU serverAuth needs to be added to the certificate. ", Output.WRONG);
            } else {
                logger.error("[3] EKU: clientAuth is absent ");
                logger.error("    Add clientAuth to the EKU of the certificate\n");
                return new Tuple<>("[3] EKU clientAuth needs to be added to the certificate", Output.WRONG);
            }

        } else {
            logger.warn("[3] EKU: No extended key usage found. Add serverAuth and clientAuth usage to the EKU of the certificate.\n");
            return new Tuple<>("[3] EKUs serverAuth and clientAuth needs to be added to the certificate. ", Output.NEEDS_ATTENTION);
        }
    }

    private static Tuple<String, Output> checkValidity(X509Certificate x509Certificate) {
        String message;
        try {
            x509Certificate.checkValidity();
            logger.info("[4] Validity: Certificate is VALID");

            DateFormat dateFormat = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy");
            Date dateObj = new Date();
            Date expiry = x509Certificate.getNotAfter();

            long mSecTillExpiry = Math.abs(expiry.getTime() - dateObj.getTime());
            long daysTillExpiry = TimeUnit.DAYS.convert(mSecTillExpiry, TimeUnit.MILLISECONDS);

            if (daysTillExpiry < 30) {
                logger.warn("    Certificate expires in less than 30 days\n");
            } else if (daysTillExpiry < 60) {
                logger.warn("    Certificate expires in less than 60 days\n");
            } else if (daysTillExpiry < 90) {
                logger.warn("    Certificate expires in less than 90 days\n");
            } else {
                logger.info("    Certificate expires in " + daysTillExpiry + "  days\n");
            }
            return new Tuple<>("[4] Certificate is VALID", Output.CORRECT);
        } catch (CertificateExpiredException e) {
            message = "[4] Validity: Certificate is INVALID: Validity date expired " + x509Certificate.getNotAfter();
        } catch (CertificateNotYetValidException e) {
            message = "[4] Validity: Certificate is INVALID: Certificate is not valid before " + x509Certificate.getNotBefore();
        }
        logger.error(message + "\n");
        return new Tuple<>(message, Output.WRONG);
    }

    private static Tuple<String, Output> checkKeySize(X509Certificate x509Certificate) {
        PublicKey publicKey = x509Certificate.getPublicKey();

        String finding = "[5] ";
        String padding = "    ";
        Output output;
        String message;

        // Determine key length and print
        int keyLength = determineKeyLength(publicKey);
        String keyLengthMessage = publicKey.getAlgorithm() + " Key length: " + keyLength;
        logger.info(padding + keyLengthMessage);

        // If unsupported key algorithm, print warning
        if (!(publicKey instanceof RSAPublicKey || publicKey instanceof DSAPublicKey)) {
            //TODO: Add different algorithm key length checks
            message = finding + keyLengthMessage;
            logger.warn(finding + "Key length not checked for " + publicKey.getAlgorithm() + "\n");
            output = Output.NEEDS_ATTENTION;
        } else {
            // If supported key length, check for validity
            if (keyLength >= 2048) {
                message = finding + "Key length: " + keyLength + " for algorithm " + publicKey.getAlgorithm() + " is VALID";
                logger.info(message + "\n");
                output = Output.CORRECT;
            } else {
                message = finding + "Key length: " + keyLength + " for algorithm " + publicKey.getAlgorithm() + " is INVALID (key length below minimum 2048 bits)";
                logger.error(message + "\n");
                output = Output.WRONG;
            }
        }
        return new Tuple<>(message, output);
    }

    private static Tuple<String, Output> checkSignature(List<X509Certificate> certificateList, X509Certificate x509Certificate) {
        String number = "[6] ";
        String message;
        Output output;
        if (TlsHelper.verifyCertificateSignature(x509Certificate, certificateList)) {
            message = number + "Signature is VALID";
            logger.info(message + "\n");
            output = Output.CORRECT;
        } else {
            message = number + "Signature is INVALID";
            logger.error(message + "\n");
            output = Output.WRONG;
        }
        return new Tuple<>(message, output);
    }


    private static int determineKeyLength(PublicKey publicKey) {
        switch (publicKey.getAlgorithm().toUpperCase()) {
            case "RSA":
                return ((RSAPublicKey) publicKey).getModulus().bitLength();
            case "DSA":
                return ((DSAPublicKey) publicKey).getParams().getP().bitLength();
            case "EC":
                return ((BCECPublicKey) publicKey).getParameters().getCurve().getFieldSize();
            default:
                logger.warn("Cannot determine key length for unknown algorithm " + publicKey.getAlgorithm());
                return -1;
        }
    }

    private Tuple<String, KeyStore.Entry> retrieveEntryFromKeystore(KeyStore.PasswordProtection keystorePasswordProtection, String alias) {
        try {
            return new Tuple<String, KeyStore.Entry>(alias, keystore.getEntry(alias, keystorePasswordProtection));
        } catch (NoSuchAlgorithmException | UnrecoverableEntryException | KeyStoreException e) {
            e.getLocalizedMessage();
            return null;
        }
    }

    public static KeyStore checkPasswordForKeystoreAndLoadKeystore(char[] keyStorePassword, String keyStorePath, String keyStoreType) {
        try {
            KeyStore keystore = KeyStoreUtils.loadKeyStore(keyStorePath, keyStorePassword, keyStoreType);
            logger.info("Password for " + keyStorePath + " in nifi.properties is VALID\n");
            return keystore;
        } catch (TlsException e) {
            logger.error("Password for " + keyStorePath + " in nifi.properties is INVALID\n");
            return null;
        }
    }

    /**
     * Checks if file exists or not in given filePath
     *
     * @param filePath file path of file to check
     *                 If file doesn't exist in filePath,
     *                 checks @param possiblePath for all files with @param requiredExtension
     * @return true if file exists
     */

    public static boolean doesFileExist(String filePath, String possiblePath, String requiredExtension) {
        File filePathFile;
        if ((filePathFile = new File(filePath)).exists()) {
            logger.info("Found " + filePathFile.getName() + " in " + filePath + "\n");
            return true;
        } else {
            //TODO: Find unicode emojis for better readability
            logger.error("Cannot load " + filePath);
            logger.error("Check for permissions of " + filePath);
            File possibleDirectoryFile = new File(possiblePath);
            logger.info("Scanning " + possibleDirectoryFile.getParent() + " for other possible " + requiredExtension + " files");
            File directoryPath = new File((possibleDirectoryFile.getParent()));
            FilenameFilter jksFilter = (dir, name) -> name.endsWith(requiredExtension);
            String fileList[] = directoryPath.list(jksFilter);
            String printOutFiles = "";
            for (String x : fileList) {
                printOutFiles += "'" + x + "' ";
            }
            logger.info("Available '" + requiredExtension + "' files:   " + printOutFiles);
            return false;
        }
    }

    /**
     * Loads the {@link NiFiProperties} instance from the provided file path .
     *
     * @return the NiFiProperties instance
     * @throws IOException               if bootstrap.conf is not present in the directory of nifi.properties file
     * @throws CommandLineParseException if nifi.properties is absent or cannot be read
     */
    public NiFiProperties loadNiFiProperties() throws IOException, CommandLineParseException {
        File nifiPropertiesFile;

        if ((nifiPropertiesFile = new File(niFiPropertiesPath)).exists()) {
            //Load encrypted nifi.properties file with key in bootstrap.conf
            try {
                String keyHex = CryptoUtils.extractKeyFromBootstrapFile(bootstrapPath);
                return NiFiPropertiesLoader.withKey(keyHex).load(nifiPropertiesFile);
            } catch (IOException e) {
                logger.error("Encountered an exception loading the default nifi.properties file with the key provided in bootstap.conf");
                logger.error("Check if bootstrap.conf is in " + nifiPropertiesFile.getParent());
                throw e;
            }
        } else {
            printAndThrowParsingException("Cannot load NiFiProperties from " + niFiPropertiesPath, ExitCode.ERROR_READING_NIFI_PROPERTIES);
            return null;
        }
    }
}
