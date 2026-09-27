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

public class ESCInvalidAttributeWriter {
    public static void main(String[] args) throws IOException {
        final String path = System.getenv().get("NIFI_ESC_ATTRIBUTE_STORAGE");

        // Print the path so the caller can verify the file is removed after execution
        System.out.print(path);

        Files.writeString(Path.of(path), "{\"attr.test\": ");
    }
}
