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
package org.apache.nifi.processors.standard.ftp.commands;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.DefaultFtpReply;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class FtpCommandHELP extends AbstractCommand {

    private static Map<String, String> COMMAND_SPECIFIC_HELP;
    private static int MAX_NUMBER_OF_COMMANDS_IN_A_ROW = 5;
    private Set<String> availableCommands = new TreeSet<>();

    static {
        Map<String, String> commands = new HashMap<>();
        commands.put("ABOR", "Syntax: ABOR");
        commands.put("APPE", "Syntax: APPE <sp> <pathname>");
        commands.put("AUTH", "Syntax: AUTH <sp> <security_mechanism>");
        commands.put("CDUP", "Syntax: CDUP");
        commands.put("CWD", "Syntax: CWD <sp> <pathname>");
        commands.put("DELE", "Syntax: DELE <sp> <pathname>");
        commands.put("EPRT", "Syntax: EPRT<space><d><net-prt><d><net-addr><d><tcp-port><d>");
        commands.put("EPSV", "Syntax: EPSV");
        commands.put("FEAT", "Syntax: FEAT");
        commands.put("HELP", "Syntax: HELP [<sp> <string>]");
        commands.put("LIST", "Syntax: LIST [<sp> <pathname>]");
        commands.put("MDTM", "Syntax: MDTM <sp> <pathname>");
        commands.put("MKD", "Syntax: MKD <sp> <pathname>");
        commands.put("MLSD", "Syntax: MLSD [<sp> <pathname>]");
        commands.put("MLST", "Syntax: MLST [<sp> <pathname>]");
        commands.put("MODE", "Syntax: MODE <sp> <mode-code>");
        commands.put("NLST", "Syntax: NLST [<sp> <pathname>]");
        commands.put("NOOP", "Syntax: NOOP");
        commands.put("OPTS", "Syntax: OPTS <sp> <options>");
        commands.put("PASS", "Syntax: PASS <sp> <password>");
        commands.put("PASV", "Syntax: PASV");
        commands.put("PBSZ", "Syntax: PBSZ <sp> <buffer_size>");
        commands.put("PORT", "Syntax: PORT <sp> <host-port>");
        commands.put("PROT", "Syntax: PROT <sp> <protection_level>");
        commands.put("PWD", "Syntax: PWD");
        commands.put("QUIT", "Syntax: QUIT");
        commands.put("REIN", "Syntax: REIN");
        commands.put("REST", "Syntax: REST <sp> <marker>");
        commands.put("RETR", "Syntax: RETR <sp> <pathname>");
        commands.put("RMD", "Syntax: RMD <sp> <pathname>");
        commands.put("RNFR", "Syntax: RNFR <sp> <pathname>");
        commands.put("RNTO", "Syntax: RNTO <sp> <pathname>");
        commands.put("SITE", "Syntax: SITE <sp> <string>");
        commands.put("SIZE", "Syntax: SIZE <sp> <pathname>");
        commands.put("STAT", "Syntax: STAT [<sp> <pathname>]");
        commands.put("STOR", "Syntax: STOR <sp> <pathname>");
        commands.put("STOU", "Syntax: STOU");
        commands.put("SYST", "Syntax: SYST");
        commands.put("TYPE", "Syntax: TYPE <sp> <type-code>");
        commands.put("USER", "Syntax: USER <sp> <username>");
        COMMAND_SPECIFIC_HELP = Collections.unmodifiableMap(commands);
    }

    public void addCommand(String command) {
        if (!command.startsWith("SITE_")) { // Parameterized commands of SITE will not appear in the general help.
            availableCommands.add(command);
        }
    }

    public void execute(final FtpIoSession session,
                        final FtpServerContext context, final FtpRequest request) {

        // reset state variables
        session.resetState();

        if (!request.hasArgument()) {
            sendDefaultHelpMessage(session);
        } else {
            handleRequestWithArgument(session, request);
        }
    }

    private void sendDefaultHelpMessage(FtpIoSession session) {
        sendCustomHelpMessage(session, getDefaultHelpMessage());
    }

    private String getDefaultHelpMessage() {
        StringBuffer helpMessage = new StringBuffer("The following commands are supported.\n");
        int currentNumberOfCommandsInARow = 0;
        Iterator<String> iterator = availableCommands.iterator();
        while (iterator.hasNext()) {
            String command = iterator.next();
            if (currentNumberOfCommandsInARow == MAX_NUMBER_OF_COMMANDS_IN_A_ROW) {
                helpMessage.append("\n");
                currentNumberOfCommandsInARow = 0;
            }
            if (iterator.hasNext()) {
                helpMessage.append(command + ", ");
            } else {
                helpMessage.append(command);
            }
            ++currentNumberOfCommandsInARow;
        }
        helpMessage.append("\nEnd of help.");
        return helpMessage.toString();
    }

    private void sendCustomHelpMessage(FtpIoSession session, String message) {
        session.write(new DefaultFtpReply(FtpReply.REPLY_214_HELP_MESSAGE, message));
    }

    private void handleRequestWithArgument(FtpIoSession session, FtpRequest request) {
        // Send command-specific help if available
        String ftpCommand = request.getArgument().toUpperCase();
        String commandSpecificHelp = null;

        if (availableCommands.contains(ftpCommand)) {
            commandSpecificHelp = COMMAND_SPECIFIC_HELP.get(ftpCommand);
        }

        if (commandSpecificHelp == null) {
            sendDefaultHelpMessage(session);
        } else {
            sendCustomHelpMessage(session, commandSpecificHelp);
        }
    }
}
