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

const target = {
    target: 'https://localhost:8443',
    secure: false,
    logLevel: 'debug',
    changeOrigin: true,
    headers: {
        'X-ProxyScheme': 'http',
        'X-ProxyPort': 4203
    },
    configure: (proxy, _options) => {
        proxy.on('error', (err, _req, _res) => {
            console.log('proxy error', err);
        });
        proxy.on('proxyReq', (proxyReq, req, _res) => {
            console.log('Sending Request to the Target:', req.method, req.url);
        });
        proxy.on('proxyRes', (proxyRes, req, _res) => {
            console.log('Received Response from the Target:', proxyRes.statusCode, req.url);
        });
    },
    bypass: function (req) {
        if (!req.url.includes('/api/') && !req.url.includes('/nifi-api/')) {
            return req.url;
        }
    }
};

export default {
    '/**': target
};
