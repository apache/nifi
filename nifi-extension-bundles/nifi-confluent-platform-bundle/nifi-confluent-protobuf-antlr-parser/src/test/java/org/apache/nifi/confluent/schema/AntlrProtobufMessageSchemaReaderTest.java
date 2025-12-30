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
package org.apache.nifi.confluent.schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for AntlrProtobufMessageSchemaParser implementation
 */
class AntlrProtobufMessageSchemaReaderTest {

    private AntlrProtobufMessageSchemaParser reader;

    @BeforeEach
    void setUp() {
        reader = new AntlrProtobufMessageSchemaParser();
    }

    @Test
    void testSimpleMessageWithNoPackage() {
        final String schema = """
            syntax = "proto3";
            message User {
                string name = 1;
            }
            """;

        final List<ProtobufMessageSchema> messages = reader.parse(schema);

        assertNotNull(messages);
        assertEquals(1, messages.size());

        final ProtobufMessageSchema user = messages.getFirst();
        assertEquals("User", user.getName());
        assertTrue(user.getPackageName().isEmpty());
        assertTrue(user.getChildMessageSchemas().isEmpty());
    }

    @Test
    void testSimpleMessageWithPackage() {
        final String schema = """
            syntax = "proto3";
            package com.example.test;
            message User {
                string name = 1;
            }
            """;

        final List<ProtobufMessageSchema> messages = reader.parse(schema);

        assertNotNull(messages);
        assertEquals(1, messages.size());

        final ProtobufMessageSchema user = messages.getFirst();
        assertEquals("User", user.getName());
        assertEquals(Optional.of("com.example.test"), user.getPackageName());
        assertTrue(user.getChildMessageSchemas().isEmpty());
    }

    @Test
    void testMultipleTopLevelGetChildMessages() {
        final String schema = """
            syntax = "proto3";
            package com.example.test;
            message User {
                string name = 1;
            }
            message Company {
                string name = 1;
            }
            message Project {
                string title = 1;
            }
            """;

        final List<ProtobufMessageSchema> messages = reader.parse(schema);

        assertNotNull(messages);
        assertEquals(3, messages.size());

        final ProtobufMessageSchema user = messages.getFirst();
        assertEquals("User", user.getName());
        assertEquals(Optional.of("com.example.test"), user.getPackageName());
        assertTrue(user.getChildMessageSchemas().isEmpty());

        final ProtobufMessageSchema company = messages.get(1);
        assertEquals("Company", company.getName());
        assertEquals(Optional.of("com.example.test"), company.getPackageName());
        assertTrue(company.getChildMessageSchemas().isEmpty());

        final ProtobufMessageSchema project = messages.get(2);
        assertEquals("Project", project.getName());
        assertEquals(Optional.of("com.example.test"), company.getPackageName());
        assertTrue(project.getChildMessageSchemas().isEmpty());
    }

    @Test
    void testNestedGetChildMessages() {
        final String schema = """
            syntax = "proto3";
            package com.example.test;
            message User {
                string name = 1;
                message Profile {
                    string bio = 1;
                    message Settings {
                        bool notifications_enabled = 1;
                    }
                }
            }
            """;

        final List<ProtobufMessageSchema> messages = reader.parse(schema);

        assertNotNull(messages);
        assertEquals(1, messages.size());

        final ProtobufMessageSchema user = messages.getFirst();
        assertEquals("User", user.getName());
        assertEquals(Optional.of("com.example.test"), user.getPackageName());
        assertEquals(1, user.getChildMessageSchemas().size());

        final ProtobufMessageSchema profile = user.getChildMessageSchemas().getFirst();
        assertEquals("Profile", profile.getName());
        assertEquals(Optional.of("com.example.test"), profile.getPackageName());
        assertEquals(1, profile.getChildMessageSchemas().size());

        final ProtobufMessageSchema settings = profile.getChildMessageSchemas().getFirst();
        assertEquals("Settings", settings.getName());
        assertEquals(Optional.of("com.example.test"), settings.getPackageName());
        assertTrue(settings.getChildMessageSchemas().isEmpty());
    }

    @Test
    void testComplexNestedStructure() {
        final String schema = """
            syntax = "proto3";
            package com.example.company;
            message Company {
                string name = 1;
                repeated Department departments = 2;
                message Department {
                    string name = 1;
                    Employee manager = 2;
                    repeated Employee employees = 3;
                    message Employee {
                        string name = 1;
                        string email = 2;
                        Position position = 3;
                        message Position {
                            string title = 1;
                            int32 level = 2;
                            double salary = 3;
                        }
                    }
                }
            }
            message Address {
                string street = 1;
                string city = 2;
                string country = 3;
            }
            """;

        final List<ProtobufMessageSchema> messages = reader.parse(schema);

        assertNotNull(messages);
        assertEquals(2, messages.size());

        // Test Company message and its nested structure
        final ProtobufMessageSchema company = messages.getFirst();
        assertEquals("Company", company.getName());
        assertEquals(Optional.of("com.example.company"), company.getPackageName());
        assertEquals(1, company.getChildMessageSchemas().size());

        final ProtobufMessageSchema department = company.getChildMessageSchemas().getFirst();
        assertEquals("Department", department.getName());
        assertEquals(Optional.of("com.example.company"), department.getPackageName());
        assertEquals(1, department.getChildMessageSchemas().size());

        final ProtobufMessageSchema employee = department.getChildMessageSchemas().getFirst();
        assertEquals("Employee", employee.getName());
        assertEquals(Optional.of("com.example.company"), employee.getPackageName());
        assertEquals(1, employee.getChildMessageSchemas().size());

        final ProtobufMessageSchema position = employee.getChildMessageSchemas().getFirst();
        assertEquals("Position", position.getName());
        assertEquals(Optional.of("com.example.company"), position.getPackageName());
        assertTrue(position.getChildMessageSchemas().isEmpty());

        // Test Address message (top-level)
        final ProtobufMessageSchema address = messages.get(1);
        assertEquals("Address", address.getName());
        assertEquals("com.example.company", address.getPackageName().orElse(""));
        assertTrue(address.getChildMessageSchemas().isEmpty());
    }

    @Test
    void testMultipleNestedGetChildMessagesInSameParent() {
        final String schema = """
            syntax = "proto3";
            package com.example.test;
            message User {
                string name = 1;
                message Profile {
                    string bio = 1;
                }
                message Settings {
                    bool notifications = 1;
                }
                message Preferences {
                    string theme = 1;
                }
            }
            """;

        final List<ProtobufMessageSchema> messages = reader.parse(schema);

        assertNotNull(messages);
        assertEquals(1, messages.size());

        final ProtobufMessageSchema user = messages.getFirst();
        assertEquals("User", user.getName());
        assertEquals(Optional.of("com.example.test"), user.getPackageName());
        assertEquals(3, user.getChildMessageSchemas().size());

        final ProtobufMessageSchema profile = user.getChildMessageSchemas().getFirst();
        assertEquals("Profile", profile.getName());
        assertEquals(Optional.of("com.example.test"), profile.getPackageName());
        assertTrue(profile.getChildMessageSchemas().isEmpty());

        final ProtobufMessageSchema settings = user.getChildMessageSchemas().get(1);
        assertEquals("Settings", settings.getName());
        assertEquals(Optional.of("com.example.test"), settings.getPackageName());
        assertTrue(settings.getChildMessageSchemas().isEmpty());

        final ProtobufMessageSchema preferences = user.getChildMessageSchemas().get(2);
        assertEquals("Preferences", preferences.getName());
        assertEquals(Optional.of("com.example.test"), preferences.getPackageName());
        assertTrue(preferences.getChildMessageSchemas().isEmpty());
    }

    @Test
    void testSchemaWithCommentsAndOptions() {
        final String schema = """
            syntax = "proto3";
            // This is a test package
            package com.example.test;
            option java_package = "com.example.test";
            option java_outer_classname = "TestProto";
            // User message definition
            message User {
                // User's name
                string name = 1;
                // User's age
                int32 age = 2;
            }
            """;

        final List<ProtobufMessageSchema> messages = reader.parse(schema);

        assertNotNull(messages);
        assertEquals(1, messages.size());

        final ProtobufMessageSchema user = messages.getFirst();
        assertEquals("User", user.getName());
        assertEquals(Optional.of("com.example.test"), user.getPackageName());
        assertTrue(user.getChildMessageSchemas().isEmpty());
    }

    @Test
    void testEmptySchema() {
        final String schema = """
            syntax = "proto3";
            """;

        final List<ProtobufMessageSchema> messages = reader.parse(schema);

        assertNotNull(messages);
        assertTrue(messages.isEmpty());
    }

    @Test
    void testSchemaWithOnlyEnums() {
        final String schema = """
            syntax = "proto3";
            package com.example.test;
            enum Status {
                UNKNOWN = 0;
                ACTIVE = 1;
                INACTIVE = 2;
            }
            enum Priority {
                LOW = 0;
                MEDIUM = 1;
                HIGH = 2;
            }
            """;

        final List<ProtobufMessageSchema> messages = reader.parse(schema);

        assertNotNull(messages);
        assertTrue(messages.isEmpty());
    }

    @Test
    void testInvalidSyntax() {
        final String schema = """
            syntax = "proto3";
            message User {
                string name = 1
                // Missing semicolon
            }
            """;

        assertThrows(RuntimeException.class, () -> reader.parse(schema));
    }

    @Test
    void testDefaultPackage() {
        final String schema = """
            syntax = "proto3";
            message User {
                string name = 1;
            }
            message Product {
                string name = 1;
            }
            """;

        final List<ProtobufMessageSchema> messages = reader.parse(schema);

        assertNotNull(messages);
        assertEquals(2, messages.size());

        final ProtobufMessageSchema user = messages.getFirst();
        assertEquals("User", user.getName());
        assertTrue(user.getPackageName().isEmpty());
        assertTrue(user.isDefaultPackage());

        final ProtobufMessageSchema product = messages.get(1);
        assertEquals("Product", product.getName());
        assertTrue(product.getPackageName().isEmpty());
        assertTrue(product.isDefaultPackage());
    }
}
