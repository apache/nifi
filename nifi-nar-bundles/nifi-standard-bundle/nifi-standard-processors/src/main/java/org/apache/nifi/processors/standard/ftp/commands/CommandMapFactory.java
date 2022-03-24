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

import org.apache.ftpserver.command.Command;
import org.apache.ftpserver.command.impl.ABOR;
import org.apache.ftpserver.command.impl.AUTH;
import org.apache.ftpserver.command.impl.CDUP;
import org.apache.ftpserver.command.impl.CWD;
import org.apache.ftpserver.command.impl.EPRT;
import org.apache.ftpserver.command.impl.EPSV;
import org.apache.ftpserver.command.impl.FEAT;
import org.apache.ftpserver.command.impl.LIST;
import org.apache.ftpserver.command.impl.MDTM;
import org.apache.ftpserver.command.impl.MKD;
import org.apache.ftpserver.command.impl.MLSD;
import org.apache.ftpserver.command.impl.MLST;
import org.apache.ftpserver.command.impl.MODE;
import org.apache.ftpserver.command.impl.NLST;
import org.apache.ftpserver.command.impl.NOOP;
import org.apache.ftpserver.command.impl.OPTS;
import org.apache.ftpserver.command.impl.PASS;
import org.apache.ftpserver.command.impl.PASV;
import org.apache.ftpserver.command.impl.PBSZ;
import org.apache.ftpserver.command.impl.PORT;
import org.apache.ftpserver.command.impl.PROT;
import org.apache.ftpserver.command.impl.PWD;
import org.apache.ftpserver.command.impl.QUIT;
import org.apache.ftpserver.command.impl.REIN;
import org.apache.ftpserver.command.impl.RMD;
import org.apache.ftpserver.command.impl.SITE;
import org.apache.ftpserver.command.impl.SITE_DESCUSER;
import org.apache.ftpserver.command.impl.SITE_HELP;
import org.apache.ftpserver.command.impl.SITE_STAT;
import org.apache.ftpserver.command.impl.SITE_WHO;
import org.apache.ftpserver.command.impl.SITE_ZONE;
import org.apache.ftpserver.command.impl.SIZE;
import org.apache.ftpserver.command.impl.STAT;
import org.apache.ftpserver.command.impl.STRU;
import org.apache.ftpserver.command.impl.SYST;
import org.apache.ftpserver.command.impl.TYPE;
import org.apache.ftpserver.command.impl.USER;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class CommandMapFactory {

    private final Map<String, Command> commandMap = new HashMap<>();
    private final FtpCommandHELP customHelpCommand = new FtpCommandHELP();
    private final AtomicReference<ProcessSessionFactory> sessionFactory;
    private final CountDownLatch sessionFactorySetSignal;
    private final Relationship relationshipSuccess;

    public CommandMapFactory(AtomicReference<ProcessSessionFactory> sessionFactory, CountDownLatch sessionFactorySetSignal, Relationship relationshipSuccess) {
        this.sessionFactory = sessionFactory;
        this.sessionFactorySetSignal = sessionFactorySetSignal;
        this.relationshipSuccess = relationshipSuccess;
    }

    public Map<String, Command> createCommandMap() {
        addToCommandMap("ABOR", new ABOR());
        addToCommandMap("ACCT", new NotSupportedCommand("Operation (ACCT) not supported."));
        addToCommandMap("APPE", new NotSupportedCommand("Operation (APPE) not supported."));
        addToCommandMap("AUTH", new AUTH());
        addToCommandMap("CDUP", new CDUP());
        addToCommandMap("CWD", new CWD());
        addToCommandMap("DELE", new NotSupportedCommand("Operation (DELE) not supported."));
        addToCommandMap("EPRT", new EPRT());
        addToCommandMap("EPSV", new EPSV());
        addToCommandMap("FEAT", new FEAT());
        addToCommandMap("HELP", customHelpCommand);
        addToCommandMap("LIST", new LIST());
        addToCommandMap("MFMT", new NotSupportedCommand("Operation (MFMT) not supported."));
        addToCommandMap("MDTM", new MDTM());
        addToCommandMap("MLST", new MLST());
        addToCommandMap("MKD", new MKD());
        addToCommandMap("MLSD", new MLSD());
        addToCommandMap("MODE", new MODE());
        addToCommandMap("NLST", new NLST());
        addToCommandMap("NOOP", new NOOP());
        addToCommandMap("OPTS", new OPTS());
        addToCommandMap("PASS", new PASS());
        addToCommandMap("PASV", new PASV());
        addToCommandMap("PBSZ", new PBSZ());
        addToCommandMap("PORT", new PORT());
        addToCommandMap("PROT", new PROT());
        addToCommandMap("PWD", new PWD());
        addToCommandMap("QUIT", new QUIT());
        addToCommandMap("REIN", new REIN());
        addToCommandMap("REST", new NotSupportedCommand("Operation (REST) not supported."));
        addToCommandMap("RETR", new NotSupportedCommand("Operation (RETR) not supported."));
        addToCommandMap("RMD", new RMD());
        addToCommandMap("RNFR", new NotSupportedCommand("Operation (RNFR) not supported."));
        addToCommandMap("RNTO", new NotSupportedCommand("Operation (RNTO) not supported."));
        addToCommandMap("SITE", new SITE());
        addToCommandMap("SIZE", new SIZE());
        addToCommandMap("SITE_DESCUSER", new SITE_DESCUSER());
        addToCommandMap("SITE_HELP", new SITE_HELP());
        addToCommandMap("SITE_STAT", new SITE_STAT());
        addToCommandMap("SITE_WHO", new SITE_WHO());
        addToCommandMap("SITE_ZONE", new SITE_ZONE());

        addToCommandMap("STAT", new STAT());
        addToCommandMap("STOR", new FtpCommandSTOR(sessionFactory, sessionFactorySetSignal, relationshipSuccess));
        addToCommandMap("STOU", new NotSupportedCommand("Operation (STOU) not supported."));
        addToCommandMap("STRU", new STRU());
        addToCommandMap("SYST", new SYST());
        addToCommandMap("TYPE", new TYPE());
        addToCommandMap("USER", new USER());

        return commandMap;
    }

    private void addToCommandMap(String command, Command instance) {
        commandMap.put(command, instance);
        if (!(instance instanceof NotSupportedCommand)) {
            customHelpCommand.addCommand(command);
        }
    }

}
