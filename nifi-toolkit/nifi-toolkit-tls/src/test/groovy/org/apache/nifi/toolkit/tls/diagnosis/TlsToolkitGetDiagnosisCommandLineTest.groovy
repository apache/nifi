package org.apache.nifi.toolkit.tls.diagnosis

import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException
import org.apache.nifi.toolkit.tls.commandLine.ExitCode
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
class TlsToolkitGetDiagnosisCommandLineTest extends GroovyTestCase {
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
    void shouldExitMainWithNoArgs() {

        //Arrange
        exit.expectSystemExitWithStatus(ExitCode.INVALID_ARGS.ordinal())
        exit.checkAssertionAfterwards({
            assert true
        })

        //exit.checkAssertionAfterwards(new VedaAssertion())

            //Act
        TlsToolkitGetDiagnosisCommandLine.main([] as String[])

        //Assert


    }


    @Test
    void testShouldFailToChooseMainWithNoOrWrongArguments() {

        //Arrange
        TlsToolkitGetDiagnosisCommandLine diagnosisCommandLine = new TlsToolkitGetDiagnosisCommandLine()

        //Act
        def msgNoArgs = shouldFail(CommandLineParseException){
            diagnosisCommandLine.chooseMain([] as String[])
        }
        def msgWrongArgs = shouldFail(CommandLineParseException){
            diagnosisCommandLine.chooseMain("wrongservice" as String[])
        }
        logger.expected(msgNoArgs)
        logger.expected(msgWrongArgs)

        //Assert
        assert msgNoArgs == "Available diagnosis on 'standalone'" as String
        assert msgWrongArgs == "No such diagnosis available. Available diagnosis: 'standalone'" as String

    }

}

/*
public class VedaAssertion implements Assertion{

    public void checkAssertion(){
        System.out.println("In VedaAssertion")
        assert true

    }



}*/
