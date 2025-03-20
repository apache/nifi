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

import { Component, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../state';
import { loadExtensionTypesForSettings } from '../../../state/extension-types/extension-types.actions';

@Component({
    selector: 'settings',
    templateUrl: './settings.component.html',
    styleUrls: ['./settings.component.scss'],
    standalone: false
})
export class Settings implements OnInit {
    tabLinks: any[] = [
        {
            label: 'General',
            link: 'general'
        },
        {
            label: 'Management Controller Services',
            link: 'management-controller-services'
        },
        {
            label: 'Reporting Tasks',
            link: 'reporting-tasks'
        },
        {
            label: 'Flow Analysis Rules',
            link: 'flow-analysis-rules'
        },
        {
            label: 'Registry Clients',
            link: 'registry-clients'
        },
        {
            label: 'Parameter Providers',
            link: 'parameter-providers'
        }
    ];

    constructor(private store: Store<NiFiState>) {}

    ngOnInit(): void {
        this.store.dispatch(loadExtensionTypesForSettings());
    }
}
