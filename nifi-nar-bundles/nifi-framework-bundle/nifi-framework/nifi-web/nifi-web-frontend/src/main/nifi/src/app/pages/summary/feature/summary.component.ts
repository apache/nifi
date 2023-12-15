/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { Component, OnDestroy, OnInit } from '@angular/core';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../state';
import { startUserPolling, stopUserPolling } from '../../../state/user/user.actions';
import { loadSummaryListing } from '../state/summary-listing/summary-listing.actions';

interface TabLink {
    label: string;
    link: string;
}

@Component({
    selector: 'summary',
    templateUrl: './summary.component.html',
    styleUrls: ['./summary.component.scss']
})
export class Summary implements OnInit, OnDestroy {
    tabLinks: TabLink[] = [
        { label: 'Processors', link: 'processors' },
        { label: 'Input Ports', link: 'input-ports' },
        { label: 'Output Ports', link: 'output-ports' },
        { label: 'Remote Process Groups', link: 'remote-process-groups' },
        { label: 'Connections', link: 'connections' },
        { label: 'Process Groups', link: 'process-groups' }
    ];

    constructor(private store: Store<NiFiState>) {}

    ngOnInit(): void {
        this.store.dispatch(startUserPolling());
        this.store.dispatch(loadSummaryListing({ recursive: true }));
    }

    ngOnDestroy(): void {
        this.store.dispatch(stopUserPolling());
    }
}
