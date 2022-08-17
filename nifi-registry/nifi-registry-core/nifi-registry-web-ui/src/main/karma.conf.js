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

const webpackConfig = require('./webpack.karma');
const path = require('path');

delete webpackConfig.entry;
delete webpackConfig.optimization;

module.exports = function (config) {

    config.set({
        basePath: './',
        browserNoActivityTimeout: 300000, //default 10000
        browserDisconnectTimeout: 180000, // default 2000
        browserDisconnectTolerance: 3, // default 0
        captureTimeout: 180000,
        frameworks: ['jasmine'],
        customLaunchers: {
            Chrome_travis_ci: {
                base: 'ChromeHeadless',
                flags: ['--no-sandbox']
            }
        },
        plugins: [
            'karma-jasmine',
            'karma-chrome-launcher',
            'karma-jasmine-html-reporter',
            'karma-spec-reporter',
            'karma-coverage',
            'karma-webpack',
            'karma-coverage-istanbul-reporter'
        ],

        files: [
            // Zone JS
            {pattern: 'node_modules/zone.js/dist/zone.min.js', included: true, watched: false},
            {pattern: 'node_modules/zone.js/dist/proxy.min.js', included: true, watched: false},
            {pattern: 'node_modules/zone.js/dist/sync-test.js', included: true, watched: false},
            {pattern: 'node_modules/zone.js/dist/jasmine-patch.min.js', included: true, watched: false},
            {pattern: 'node_modules/zone.js/dist/async-test.js', included: true, watched: false},
            {pattern: 'node_modules/zone.js/dist/fake-async-test.js', included: true, watched: false},

            // Including spec files for running
            {pattern: 'karma-test-shim.js', included: true, watched: true}

        ],

        webpack: webpackConfig,

        webpackServer: {
            noInfo: true
        },

        webpackMiddleware: {
            stats: 'errors-only',
            logLevel: 'error'
        },

        exclude: [],

        preprocessors: {
            'karma-test-shim.js': ['webpack']
        },

        // Try Websocket for a faster transmission first. Fallback to polling if necessary.
        transports: ['websocket', 'polling'],

        reporters: ['spec', 'coverage-istanbul'],

        coverageIstanbulReporter: {
            reports: [ 'html', 'text-summary' ],

            dir: path.join(__dirname, 'coverage'),

            fixWebpackSourcePaths: true,

            'report-config': {
                html: {
                    subdir: 'html'
                }
            }
        },

        specReporter: {
            failFast: false
        },

        port: 9876,
        colors: true,
        logLevel: config.LOG_INFO,
        autoWatch: true,
        browsers: ['Chrome'],
        singleRun: false
    });

    if (process.env.TRAVIS) {
        config.set({
            browsers: ['Chrome_travis_ci']
        });

        // Override base config
        config.set({
            singleRun: true,
            autoWatch: false,
            reporters: ['spec', 'coverage'],
            specReporter: {
                failFast: true
            }
        });
    }
}

