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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class FtpCommandHELP extends AbstractCommand {

    private static final Map<String, String> COMMAND_SPECIFIC_HELP = Map.ofEntries(
            Map.entry("ABOR", "Syntax: ABOR"),
            Map.entry("APPE", "Syntax: APPE <sp> <pathname>"),
            Map.entry("AUTH", "Syntax: AUTH <sp> <security_mechanism>"),
            Map.entry("CDUP", "Syntax: CDUP"),
            Map.entry("CWD", "Syntax: CWD <sp> <pathname>"),
            Map.entry("DELE", "Syntax: DELE <sp> <pathname>"),
            Map.entry("EPRT", "Syntax: EPRT<space><d><net-prt><d><net-addr><d><tcp-port><d>"),
            Map.entry("EPSV", "Syntax: EPSV"),
            Map.entry("FEAT", "Syntax: FEAT"),
            Map.entry("HELP", "Syntax: HELP [<sp> <string>]"),
            Map.entry("LIST", "Syntax: LIST [<sp> <pathname>]"),
            Map.entry("MDTM", "Syntax: MDTM <sp> <pathname>"),
            Map.entry("MKD", "Syntax: MKD <sp> <pathname>"),
            Map.entry("MLSD", "Syntax: MLSD [<sp> <pathname>]"),
            Map.entry("MLST", "Syntax: MLST [<sp> <pathname>]"),
            Map.entry("MODE", "Syntax: MODE <sp> <mode-code>"),
            Map.entry("NLST", "Syntax: NLST [<sp> <pathname>]"),
            Map.entry("NOOP", "Syntax: NOOP"),
            Map.entry("OPTS", "Syntax: OPTS <sp> <options>"),
            Map.entry("PASS", "Syntax: PASS <sp> <password>"),
            Map.entry("PASV", "Syntax: PASV"),
            Map.entry("PBSZ", "Syntax: PBSZ <sp> <buffer_size>"),
            Map.entry("PORT", "Syntax: PORT <sp> <host-port>"),
            Map.entry("PROT", "Syntax: PROT <sp> <protection_level>"),
            Map.entry("PWD", "Syntax: PWD"),
            Map.entry("QUIT", "Syntax: QUIT"),
            Map.entry("REIN", "Syntax: REIN"),
            Map.entry("REST", "Syntax: REST <sp> <marker>"),
            Map.entry("RETR", "Syntax: RETR <sp> <pathname>"),
            Map.entry("RMD", "Syntax: RMD <sp> <pathname>"),
            Map.entry("RNFR", "Syntax: RNFR <sp> <pathname>"),
            Map.entry("RNTO", "Syntax: RNTO <sp> <pathname>"),
            Map.entry("SITE", "Syntax: SITE <sp> <string>"),
            Map.entry("SIZE", "Syntax: SIZE <sp> <pathname>"),
            Map.entry("STAT", "Syntax: STAT [<sp> <pathname>]"),
            Map.entry("STOR", "Syntax: STOR <sp> <pathname>"),
            Map.entry("STOU", "Syntax: STOU"),
            Map.entry("SYST", "Syntax: SYST"),
            Map.entry("TYPE", "Syntax: TYPE <sp> <type-code>"),
            Map.entry("USER", "Syntax: USER <sp> <username>")
    );

    private static final int MAX_NUMBER_OF_COMMANDS_IN_A_ROW = 5;
    private final Set<String> availableCommands = new TreeSet<>();

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
        StringBuilder helpMessage = new StringBuilder("The following commands are supported.\n");
        int currentNumberOfCommandsInARow = 0;
        Iterator<String> iterator = availableCommands.iterator();
        while (iterator.hasNext()) {
            String command = iterator.next();
            if (currentNumberOfCommandsInARow == MAX_NUMBER_OF_COMMANDS_IN_A_ROW) {
                helpMessage.append("\n");
                currentNumberOfCommandsInARow = 0;
            }
            if (iterator.hasNext()) {
                helpMessage.append(command).append(", ");
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
