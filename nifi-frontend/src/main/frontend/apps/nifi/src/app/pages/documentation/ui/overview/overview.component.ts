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

import { Component } from '@angular/core';
import { NiFiState } from '../../../../state';
import { Store } from '@ngrx/store';
import { ComponentType } from '@nifi/shared';
import {
    selectAllDeveloperDocumentation,
    selectAllGeneralDocumentation
} from '../../state/external-documentation/external-documentation.selectors';
import { MatCardModule } from '@angular/material/card';
import { NgTemplateOutlet } from '@angular/common';

@Component({
    selector: 'overview',
    imports: [MatCardModule, NgTemplateOutlet],
    templateUrl: './overview.component.html',
    styleUrls: ['./overview.component.scss']
})
export class Overview {
    generalDocumentation = this.store.selectSignal(selectAllGeneralDocumentation);
    developerDocumentation = this.store.selectSignal(selectAllDeveloperDocumentation);

    constructor(private store: Store<NiFiState>) {}

    protected readonly ComponentType = ComponentType;
}
