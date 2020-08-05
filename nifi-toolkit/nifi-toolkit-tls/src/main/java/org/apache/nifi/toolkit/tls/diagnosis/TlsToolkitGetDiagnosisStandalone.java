package org.apache.nifi.toolkit.tls.diagnosis;

import org.apache.commons.cli.*;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.security.kms.CryptoUtils;
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.commandLine.ExitCode;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandaloneCommandLine;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class TlsToolkitGetDiagnosisStandalone {

    private static final String NIFI_PROPERTIES_ARG = "nifiProperties";

    private String keystorePath;
    private String TRUSTSTORE_PATH;
    private String niFiPropertiesPath;
    private String bootstrapPath;
    private Options options;

    private NiFiProperties niFiProperties;


    private static final Logger logger = LoggerFactory.getLogger(TlsToolkitStandaloneCommandLine.class);

    private static Options buildOptions(){
        Options options = new Options();
        options.addOption(Option.builder("n").longOpt(NIFI_PROPERTIES_ARG).hasArg(true).argName("file").desc("This field specifies nifi.properties file name").build());
        return options;
    }

    private void parseStandaloneDiagnosis(String[] args) throws CommandLineParseException {
        CommandLineParser parser = new DefaultParser();

        try{
            CommandLine commandLine = parser.parse(options, args);

            //nifi.properties present?
            if(commandLine.hasOption(NIFI_PROPERTIES_ARG)){
                bootstrapPath = niFiPropertiesPath = commandLine.getOptionValue(NIFI_PROPERTIES_ARG);
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
        helpFormatter.printHelp(TlsToolkitGetDiagnosisStandalone.class.getCanonicalName(), buildOptions(),true);
    }

    public static void printAndThrowParsingException(String errorMessage, ExitCode exitCode) throws CommandLineParseException {
        printUsage(errorMessage);
        throw new CommandLineParseException(errorMessage, exitCode);
    }



    public static void main(String[] args) {

        TlsToolkitGetDiagnosisStandalone standalone = new TlsToolkitGetDiagnosisStandalone();
        standalone.options = buildOptions();
        try {
            standalone.parseStandaloneDiagnosis(args);
            standalone.niFiProperties = standalone.loadNiFiProperties();
        } catch (CommandLineParseException e) {
            System.exit(e.getExitCode().ordinal());
        }
        catch (IOException e){
            printUsage(e.getLocalizedMessage());
            System.exit(-1);
        }

        //Get keystore path
        standalone.keystorePath = standalone.niFiProperties.getProperty("nifi.security.keystore");


    }

    /**
     * Loads the {@link NiFiProperties} instance from the provided file path .
     *
     * @return the NiFiProperties instance
     *
     * @throws IOException if bootstrap.conf is not present in the directory of nifi.properties file
     * @throws CommandLineParseException if nifi.properties is absent or cannot be read
     */
    public NiFiProperties loadNiFiProperties() throws IOException, CommandLineParseException {
        File nifiPropertiesFile;

        if((nifiPropertiesFile = new File(niFiPropertiesPath)).exists()){

            //Load encrypted nifi.properties file with key in bootstrap.conf
            try {
                String keyHex = CryptoUtils.extractKeyFromBootstrapFile(bootstrapPath);
                return NiFiPropertiesLoader.withKey(keyHex).load(nifiPropertiesFile);
            }
            catch (IOException e) {
                logger.error("Encountered an exception loading the default nifi.properties file {} with the key provided in bootstrap.conf\n Check if bootstrap.conf is in " + nifiPropertiesFile.getParent(), CryptoUtils.getDefaultFilePath(), e);
                throw e;
            }
        }
        else{
            printAndThrowParsingException("Cannot load NiFiProperties from " + niFiPropertiesPath , ExitCode.ERROR_READING_NIFI_PROPERTIES );
            return null;
        }
    }







}
