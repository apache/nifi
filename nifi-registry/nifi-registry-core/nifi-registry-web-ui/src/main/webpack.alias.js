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

const path = require('path');

module.exports = {
    // Nifi Registry app folders
    'components': path.resolve(__dirname, 'webapp/components'),
    'services': path.resolve(__dirname, 'webapp/services'),
    'images': path.resolve(__dirname, 'webapp/images'),
    'locale': path.resolve(__dirname, 'locale'),

    // Nifi Registry app files
    'nf-registry.testbed-factory': path.resolve(__dirname, 'webapp/nf-registry.testbed-factory.js'),
    'nf-registry.animations': path.resolve(__dirname, 'webapp/nf-registry.animations.js'),
    'nf-registry.routes' : path.resolve(__dirname, 'webapp/nf-registry.routes.js'),
    'nf-registry': path.resolve(__dirname, 'webapp/nf-registry.js'),
    'nf-registry.module': path.resolve(__dirname, 'webapp/nf-registry.module.js')
};
