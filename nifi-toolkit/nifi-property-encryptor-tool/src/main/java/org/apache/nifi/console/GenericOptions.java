package org.apache.nifi.console;

import picocli.CommandLine;

@CommandLine.Command(synopsisHeading      = "%nUsage:%n%n",
        descriptionHeading   = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n%n",
        optionListHeading    = "%nOptions:%n%n",
        commandListHeading   = "%nCommands:%n%n")
public class GenericOptions {

    @CommandLine.Option(names = {"-v", "--verbose"}, description = {
            "Enable verbose logging"}, defaultValue = "false")
    boolean verboseLogging;

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = {
            "Print usage guide (this message)"})
    boolean helpRequested;
}