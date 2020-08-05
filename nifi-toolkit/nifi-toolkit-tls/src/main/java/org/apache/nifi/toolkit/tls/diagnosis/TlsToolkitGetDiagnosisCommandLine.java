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

import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException;
import org.apache.nifi.toolkit.tls.commandLine.ExitCode;
import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandaloneCommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TlsToolkitGetDiagnosisCommandLine {

    public static final String DESCRIPTION = "Diagnoses issues in common deployment scenario of TLS toolkit";
    private static final Logger logger = LoggerFactory.getLogger(TlsToolkitStandaloneCommandLine.class);


    public static void main(String[] args) {

        TlsToolkitGetDiagnosisCommandLine commandLine = new TlsToolkitGetDiagnosisCommandLine();
        try {
            commandLine.chooseMain(args);
        } catch (CommandLineParseException e) {
            System.exit(e.getExitCode().ordinal());
        }

    }

    public void chooseMain(String[] args) throws CommandLineParseException {


        if(args.length < 1){
           //How to print errors and exit
            logger.error("No diagnosis argument passed.");

            throw new CommandLineParseException("Available diagnosis on 'standalone'", ExitCode.INVALID_ARGS);

        }

        String arg1 = args[0];

        //Diagnosis for standalone NiFi
        if(arg1.toLowerCase().equals("standalone")){
            TlsToolkitGetDiagnosisStandalone.main(args);
        } else {
            logger.error("No such diagnosis mode available:");
            throw new CommandLineParseException("No such diagnosis available. Available diagnosis: 'standalone'", ExitCode.INVALID_ARGS);
        }

    }


}
