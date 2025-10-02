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

import { Component, inject } from '@angular/core';
import { MatFormField } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatInputModule } from '@angular/material/input';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { Droplet } from 'apps/nifi-registry/src/app/state/droplets';
import { Store } from '@ngrx/store';
import { exportDropletVersion } from 'apps/nifi-registry/src/app/state/droplets/droplets.actions';
import { CloseOnEscapeDialog } from '@nifi/shared';
import { MatButtonModule } from '@angular/material/button';
import { ContextErrorBanner } from '../../../../../ui/common/context-error-banner/context-error-banner.component';
import { ErrorContextKey } from '../../../../../state/error';

export interface ExportFlowVersionDialogData {
    droplet: Droplet;
}

@Component({
    selector: 'app-export-flow-version-dialog',
    imports: [MatFormField, MatSelectModule, MatInputModule, MatDialogModule, MatButtonModule, ContextErrorBanner],
    templateUrl: './export-droplet-version-dialog.component.html',
    styleUrl: './export-droplet-version-dialog.component.scss'
})
export class ExportDropletVersionDialogComponent extends CloseOnEscapeDialog {
    data = inject<ExportFlowVersionDialogData>(MAT_DIALOG_DATA);
    private store = inject(Store);

    droplet: Droplet;
    selectedVersion: number;

    constructor() {
        super();
        const data = this.data;

        this.droplet = data.droplet;
        this.selectedVersion = this.droplet.versionCount;
    }

    exportVersion() {
        this.store.dispatch(
            exportDropletVersion({ request: { droplet: this.droplet, version: this.selectedVersion } })
        );
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
