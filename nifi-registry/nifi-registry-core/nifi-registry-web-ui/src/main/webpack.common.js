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
const FixStyleOnlyEntriesPlugin = require('webpack-fix-style-only-entries');

const webpackAlias = require('./webpack.alias');
const loaders = require('./webpack.loader');

module.exports = {
    // Deployment target
    target: 'web',

    // Starting point of building the bundles
    entry: {
        // JS files
        'nf-registry.bundle.min': path.resolve(__dirname, 'webapp/nf-registry-bootstrap.js'),

        // SCSS files
        'nf-registry.style.min': [
            path.resolve(__dirname, 'webapp/theming/nf-registry.scss'),
            path.resolve(__dirname, 'node_modules/@nifi-fds/core/common/styles/flow-design-system.scss')
        ]
    },

    // Output bundles
    output: {
        // add the content hash for auto cache-busting
        filename: '[name].[contenthash].js',
        path: path.resolve(__dirname, './')
    },

    optimization: {
        splitChunks: {
            cacheGroups: {
                vendor: {
                    chunks: 'initial',
                    test: path.resolve(__dirname, 'node_modules'),
                    name: 'vendor.min',
                    enforce: true
                }
            }
        }
    },

    // Change how modules are resolved
    resolve: {
        extensions: ['.ts', '.tsx', '.js'],
        alias: webpackAlias,
        symlinks: false
    },

    // Polyfill or mock certain Node.js globals and modules
    node: {
        console: true
    },

    stats: 'errors-only',

    module: {
        rules: [
            loaders.html,
            loaders.scss,
            loaders.images,
            loaders.fonts,
            loaders.xlf
        ]
    },

    plugins: [
        // Fix style only entry generating an extra js file
        new FixStyleOnlyEntriesPlugin({ silent: true })
    ]
};
