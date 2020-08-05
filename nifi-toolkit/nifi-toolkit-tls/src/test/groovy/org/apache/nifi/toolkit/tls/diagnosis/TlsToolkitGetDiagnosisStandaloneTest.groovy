package org.apache.nifi.toolkit.tls.diagnosis

import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.Assume
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security


@RunWith(JUnit4.class)
class TlsToolkitGetDiagnosisStandaloneTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TlsToolkitGetDiagnosisCommandLineTest.class)

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none()

    @BeforeClass
    static void setUpOnce() throws Exception {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS)
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        //setupTmpDir() ???
    }

    void setUp() {
        super.setUp()
    }

    void tearDown() {
    }

    @Test
    void testPrintUsage(){

        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()

        //Act
        standalone.printUsage("This is an error message");

        //Assert
    }

    @Test
    void testShouldParseStandaloneDiagnosis(){
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        standalone.options = standalone.buildOptions()
        String args = "-n nifi.properties"

        //Act
        standalone.parseStandaloneDiagnosis(args.split(" ") as String[])

        //Assert
        assert standalone.niFiPropertiesPath == "nifi.properties"
    }

    @Test
    void testShouldFailParseStandaloneDiagnosis(){
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        standalone.options = standalone.buildOptions()
        String args = "-w wrongservice -p"

        //Act
        def msg = shouldFail(CommandLineParseException) {
            standalone.parseStandaloneDiagnosis(args.split(" ") as String[])
        }

        assert msg == "Error parsing command line. (Unrecognized option: -w)"
    }

    @Test
    void testShouldLoadNiFiProperties(){
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String niFiPropertiesPath = "src/test/resources/diagnosis/nifi.properties"
        String[] args = ["-n", niFiPropertiesPath] as String[]
        standalone.options = standalone.buildOptions()
        standalone.parseStandaloneDiagnosis(args)
        logger.info("Parsed nifi.properties location: ${standalone.niFiPropertiesPath}")

        //Act
        NiFiProperties properties = standalone.loadNiFiProperties()

        //Assert
        assert properties
        assert properties.size() > 0
    }

    @Test
    void testPrintKeystorePath(){
        //Arrange
        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone()
        String niFiPropertiesPath = "src/test/resources/diagnosis/nifi.properties"
        String[] args = ["-n", niFiPropertiesPath] as String[]
        standalone.options = standalone.buildOptions()
        standalone.parseStandaloneDiagnosis(args)
        logger.info("Parsed nifi.properties location: ${standalone.niFiPropertiesPath}")

        //Act
        NiFiProperties properties = standalone.loadNiFiProperties()
        def keystorePath = properties.getProperty("nifi.security.keystore")

        //Assert
        System.out.println(keystorePath)

    }


}
