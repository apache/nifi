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
import { CloseOnEscapeDialog } from '@nifi/shared';
import { Store } from '@ngrx/store';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { deleteDroplet } from 'apps/nifi-registry/src/app/state/droplets/droplets.actions';
import { Droplet } from 'apps/nifi-registry/src/app/state/droplets';
import { MatButtonModule } from '@angular/material/button';
import { ContextErrorBanner } from '@nifi/shared';
import { ErrorContextKey } from 'apps/nifi-registry/src/app/state/error';

interface DeleteDropletDialogData {
    droplet: Droplet;
}

@Component({
    selector: 'app-delete-droplet-dialog',
    standalone: true,
    imports: [CommonModule, MatDialogModule, MatButtonModule, ContextErrorBanner],
    templateUrl: './delete-droplet-dialog.component.html',
    styleUrl: './delete-droplet-dialog.component.scss'
})
export class DeleteDropletDialogComponent extends CloseOnEscapeDialog {
    protected readonly ErrorContextKey = ErrorContextKey;
    droplet: Droplet;

    constructor(
        @Inject(MAT_DIALOG_DATA) public data: DeleteDropletDialogData,
        private store: Store
    ) {
        super();
        this.droplet = data.droplet;
    }

    deleteDroplet(droplet: Droplet) {
        this.store.dispatch(deleteDroplet({ request: { droplet } }));
    }
}
