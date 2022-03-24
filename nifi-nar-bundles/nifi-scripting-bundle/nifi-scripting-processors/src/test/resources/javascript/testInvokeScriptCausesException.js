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

var Set = Java.type("java.util.HashSet");

var processor = new Object() {
    logger: null,

    // Processor
    initialize: function(context) {
    },

    getRelationships: function() {
        return new Set();
    },

    onTrigger: function(context, sessionFactory) {
        throw new Error("Failed!");
    },

    // ConfigurableComponent
    validate: function(context) {
        return [];
    },

    getPropertyDescriptor: function(name) {
        return null;
    },

    onPropertyModified: function(descriptor, oldValue, newValue) {
    },

    getPropertyDescriptors: function() {
        return [];
    },

    getIdentifier: function() {
        return null;
    },

    setLogger: function(logger) {
        this.logger = logger;
    }
};
