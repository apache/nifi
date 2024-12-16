/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { Config } from 'jest';

const config: Config = {
    displayName: 'Update Attribute UI',
    clearMocks: true,
    coverageDirectory: '../../coverage/apps/nifi',
    extensionsToTreatAsEsm: ['.ts'],

    // A preset that is used as a base for Jest's configuration
    preset: '../../jest.preset.js',

    // The paths to modules that run some code to configure or set up the testing environment before each test
    setupFilesAfterEnv: ['<rootDir>/setup-jest.ts'],

    // The test environment that will be used for testing
    testEnvironment: '@happy-dom/jest-environment',

    // A map from regular expressions to paths to transformers
    transform: {
        '^.+\\.(ts|mjs|js|html)$': [
            'jest-preset-angular',
            {
                tsconfig: '<rootDir>/tsconfig.spec.json',
                stringifyContentPathRegex: '\\.(html|svg)$',
                useESM: true
            }
        ]
    },

    // An array of regexp pattern strings that are matched against all source file paths, matched files will skip transformation
    transformIgnorePatterns: []
};

export default config;
