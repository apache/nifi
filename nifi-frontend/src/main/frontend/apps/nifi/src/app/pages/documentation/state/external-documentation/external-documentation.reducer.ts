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

import { ExternalDocumentationState } from './index';
import { createReducer } from '@ngrx/store';

export const initialState: ExternalDocumentationState = {
    generalDocumentation: [
        {
            name: 'overview',
            displayName: 'Overview',
            url: '../nifi-api/html/overview.html'
        },
        {
            name: 'getting-started',
            displayName: 'Getting Started',
            url: '../nifi-api/html/getting-started.html'
        },
        {
            name: 'user-guide',
            displayName: 'User Guide',
            url: '../nifi-api/html/user-guide.html'
        },
        {
            name: 'expression-language-guide',
            displayName: 'Expression Language Guide',
            url: '../nifi-api/html/expression-language-guide.html'
        },
        {
            name: 'record-path-guide',
            displayName: 'Record Path Guide',
            url: '../nifi-api/html/record-path-guide.html'
        },
        {
            name: 'admin-guide',
            displayName: 'Admin Guide',
            url: '../nifi-api/html/administration-guide.html'
        },
        {
            name: 'toolkit-guide',
            displayName: 'Toolkit Guide',
            url: '../nifi-api/html/toolkit-guide.html'
        },
        {
            name: 'walkthroughs',
            displayName: 'Walkthroughs',
            url: '../nifi-api/html/walkthroughs.html'
        }
    ],
    developerDocumentation: [
        {
            name: 'rest-api',
            displayName: 'REST API',
            url: '../nifi-api/rest-api/index.html'
        },
        {
            name: 'developer-guide',
            displayName: 'Developer Guide',
            url: '../nifi-api/html/developer-guide.html'
        },
        {
            name: 'python-developer-guide',
            displayName: 'Python Developer Guide',
            url: '../nifi-api/html/python-developer-guide.html'
        },
        {
            name: 'apache-nifi-in-depth',
            displayName: 'Apache NiFi In Depth',
            url: '../nifi-api/html/nifi-in-depth.html'
        }
    ]
};

export const externalDocumentationReducer = createReducer(initialState);
