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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class ESCAttributeUpdater {
    public static void main(String[] args) throws IOException {
        System.out.println("Hello, " + args[0]);

        updateAttributes(System.getenv().get("NIFI_ESC_ATTRIBUTE_STORAGE"));
    }

    private static void updateAttributes(String path) throws IOException {
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("attr.test", "wrote");

        // Launched as a single-file program with only the JDK available, so the JSON is written by hand
        final StringJoiner json = new StringJoiner(",", "{", "}");
        for (Map.Entry<String, Object> attr : attrs.entrySet()) {
            json.add(quote(attr.getKey()) + ":" + quote(String.valueOf(attr.getValue())));
        }
        Files.writeString(Path.of(path), json.toString());
    }

    private static String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
