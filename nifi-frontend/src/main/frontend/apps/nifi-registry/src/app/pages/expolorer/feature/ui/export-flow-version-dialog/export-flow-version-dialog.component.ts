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

import { Component, Inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatFormField } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { Droplets } from 'apps/nifi-registry/src/app/state/droplets';
import { Store } from '@ngrx/store';
import { exportFlowVersion } from 'apps/nifi-registry/src/app/state/droplets/droplets.actions';
import { CloseOnEscapeDialog } from '@nifi/shared';
import { MatButtonModule } from '@angular/material/button';

interface Data {
    droplet: Droplets;
}

@Component({
    selector: 'app-export-flow-version-dialog',
    standalone: true,
    imports: [CommonModule, MatFormField, MatSelectModule, MatInputModule, MatDialogModule, MatButtonModule],
    templateUrl: './export-flow-version-dialog.component.html',
    styleUrl: './export-flow-version-dialog.component.scss'
})
export class ExportFlowVersionDialogComponent extends CloseOnEscapeDialog {
    droplet: Droplets;
    selectedVersion: number;

    constructor(
        @Inject(MAT_DIALOG_DATA) public data: Data,
        private store: Store
    ) {
        super();
        this.droplet = data.droplet;
        this.selectedVersion = this.droplet.versionCount;
    }

    exportVersion() {
        this.store.dispatch(exportFlowVersion({ request: { droplet: this.droplet, version: this.selectedVersion } }));
    }
}
