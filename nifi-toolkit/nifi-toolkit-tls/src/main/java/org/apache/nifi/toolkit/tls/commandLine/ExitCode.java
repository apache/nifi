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

package org.apache.nifi.toolkit.tls.commandLine;

/**
 * Possible exit codes
 */
public enum ExitCode {
    /**
     * Tool ran successfully
     */
    SUCCESS,

    /**
     * Tool exited after printing help
     */
    HELP,

    /**
     * Invalid arguments passed in via command line
     */
    INVALID_ARGS,

    /**
     * Error invoking service
     */
    SERVICE_ERROR,

    /**
     * Unable to parse command line
     */
    ERROR_PARSING_COMMAND_LINE,

    /**
     * Unable to generate configuration
     */
    ERROR_GENERATING_CONFIG,

    /**
     * Specified wrong number of passwords
     */
    ERROR_INCORRECT_NUMBER_OF_PASSWORDS,

    /**
     * Expected an integer for an argument
     */
    ERROR_PARSING_INT_ARG,

    /**
     * Did not specify token
     */
    ERROR_TOKEN_ARG_EMPTY,

    /**
     * Unable to read nifi.properties
     */
    ERROR_READING_NIFI_PROPERTIES,

    /**
     * Unable to read existing configuration value or file
     */
    ERROR_READING_CONFIG
}
