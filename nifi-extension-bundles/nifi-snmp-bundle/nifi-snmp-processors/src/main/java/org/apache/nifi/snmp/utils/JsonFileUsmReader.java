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
package org.apache.nifi.snmp.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.nifi.processor.exception.ProcessException;
import org.snmp4j.security.UsmUser;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Scanner;

public class JsonFileUsmReader implements UsmReader {

    private final String usmUsersFilePath;

    public JsonFileUsmReader(String usmUsersFilePath) {
        this.usmUsersFilePath = usmUsersFilePath;
    }

    @Override
    public List<UsmUser> readUsm() {
        final List<UsmUser> userDetails;
        try (Scanner scanner = new Scanner(new File(usmUsersFilePath))) {
            final String content = scanner.useDelimiter("\\Z").next();
            userDetails = UsmJsonParser.parse(content);
        } catch (FileNotFoundException e) {
            throw new ProcessException("USM user file not found, please check the file path and file permissions.", e);
        } catch (JsonProcessingException e) {
            throw new ProcessException("Could not parse USM user file, please check the processor details for examples.", e);
        }
        return userDetails;
    }
}
