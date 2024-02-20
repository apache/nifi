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

package org.apache.nifi.minifi.toolkit.configuration;

import static org.apache.nifi.minifi.toolkit.configuration.json.TransformYamlCommandFactory.TRANSFORM_YML;
import static org.apache.nifi.minifi.toolkit.configuration.json.TransformNifiCommandFactory.TRANSFORM_NIFI;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.nifi.minifi.toolkit.configuration.json.TransformNifiCommandFactory;
import org.apache.nifi.minifi.toolkit.configuration.json.TransformYamlCommandFactory;

public class ConfigMain {
    public static final int ERR_INVALID_ARGS = 1;
    public static final int ERR_UNABLE_TO_OPEN_INPUT = 3;
    public static final int ERR_UNABLE_TO_READ_TEMPLATE = 4;
    public static final int ERR_UNABLE_TO_PARSE_CONFIG = 6;
    public static final int ERR_UNABLE_TO_SAVE_CONFIG = 9;

    public static final int SUCCESS = 0;

    private final Map<String, Command> commandMap;
    private final PathInputStreamFactory pathInputStreamFactory;
    private final PathOutputStreamFactory pathOutputStreamFactory;

    public ConfigMain(PathInputStreamFactory pathInputStreamFactory, PathOutputStreamFactory pathOutputStreamFactory) {
        this.pathInputStreamFactory = pathInputStreamFactory;
        this.pathOutputStreamFactory = pathOutputStreamFactory;
        this.commandMap = createCommandMap();
    }

    public static void main(String[] args) {
        System.exit(new ConfigMain(FileInputStream::new, FileOutputStream::new).execute(args));
    }

    public int execute(String[] args) {
        if (args.length < 1 || !commandMap.containsKey(args[0].toLowerCase())) {
            printUsage();
            return ERR_INVALID_ARGS;
        }
        return commandMap.get(args[0].toLowerCase()).function.apply(args);
    }

    private void printUsage() {
        System.out.println("Usage:");
        System.out.println();
        System.out.println("Valid commands include:");
        commandMap.forEach((s, command) -> System.out.println(s + ": " + command.description));
    }

    public static class Command {
        private final Function<String[], Integer> function;
        private final String description;

        public Command(Function<String[], Integer> function, String description) {
            this.function = function;
            this.description = description;
        }
    }

    public Map<String, Command> createCommandMap() {
        Map<String, Command> result = new TreeMap<>();
        result.put(TRANSFORM_YML,
                new TransformYamlCommandFactory(pathInputStreamFactory, pathOutputStreamFactory).create());
        result.put(TRANSFORM_NIFI,
                new TransformNifiCommandFactory(pathInputStreamFactory, pathOutputStreamFactory).create());
        return result;
    }
}
