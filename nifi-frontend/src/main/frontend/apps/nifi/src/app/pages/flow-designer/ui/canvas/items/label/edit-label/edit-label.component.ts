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
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { CanvasState } from '../../../../../state';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { Store } from '@ngrx/store';
import { updateComponent } from '../../../../../state/flow/flow.actions';
import { Client } from '../../../../../../../service/client.service';
import { EditComponentDialogRequest } from '../../../../../state/flow';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { selectSaving } from '../../../../../state/flow/flow.selectors';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';
import { MatOption } from '@angular/material/autocomplete';
import { MatSelect } from '@angular/material/select';
import { CloseOnEscapeDialog, SelectOption } from '@nifi/shared';
import { ErrorContextKey } from '../../../../../../../state/error';
import { ContextErrorBanner } from '../../../../../../../ui/common/context-error-banner/context-error-banner.component';

@Component({
    selector: 'edit-label',
    templateUrl: './edit-label.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        AsyncPipe,
        NifiSpinnerDirective,
        MatOption,
        MatSelect,
        ContextErrorBanner
    ],
    styleUrls: ['./edit-label.component.scss']
})
export class EditLabel extends CloseOnEscapeDialog {
    saving$ = this.store.select(selectSaving);

    editLabelForm: FormGroup;
    readonly: boolean;

    fontSizeOptions: SelectOption[] = [12, 14, 16, 18, 20, 22, 24].map((fontSize) => {
        return {
            text: `${fontSize}px`,
            value: `${fontSize}px`
        };
    });

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditComponentDialogRequest,
        private formBuilder: FormBuilder,
        private store: Store<CanvasState>,
        private client: Client,
        private clusterConnectionService: ClusterConnectionService
    ) {
        super();
        this.readonly = !request.entity.permissions.canWrite;

        let fontSize = this.fontSizeOptions[0].value;
        if (request.entity.component.style['font-size']) {
            fontSize = request.entity.component.style['font-size'];
        }

        // build the form
        this.editLabelForm = this.formBuilder.group({
            value: new FormControl(request.entity.component.label, Validators.required),
            fontSize: new FormControl(fontSize, Validators.required)
        });
    }

    editLabel() {
        const payload: any = {
            revision: this.client.getRevision(this.request.entity),
            disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
            component: {
                id: this.request.entity.id,
                label: this.editLabelForm.get('value')?.value,
                style: {
                    'font-size': this.editLabelForm.get('fontSize')?.value
                }
            }
        };

        this.store.dispatch(
            updateComponent({
                request: {
                    id: this.request.entity.id,
                    type: this.request.type,
                    uri: this.request.uri,
                    payload,
                    errorStrategy: 'banner'
                }
            })
        );
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
