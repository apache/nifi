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

import { Component, EventEmitter, Input, Output, inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { MatOption } from '@angular/material/autocomplete';
import { MatSelect } from '@angular/material/select';
import { Observable, of } from 'rxjs';
import { Client } from '../../../../service/client.service';
import { ClusterConnectionService } from '../../../../service/cluster-connection.service';
import { CloseOnEscapeDialog, NifiSpinnerDirective, SelectOption } from '@nifi/shared';
import { EditComponentDialogRequest, UpdateComponentRequest } from '../../../../state/shared';
import { ErrorContextKey } from '../../../../state/error';
import { ContextErrorBanner } from '../../context-error-banner/context-error-banner.component';

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
    request = inject<EditComponentDialogRequest>(MAT_DIALOG_DATA);
    private formBuilder = inject(FormBuilder);
    private client = inject(Client);
    private clusterConnectionService = inject(ClusterConnectionService);

    @Input() saving$: Observable<boolean> = of(false);
    @Output() editLabel: EventEmitter<UpdateComponentRequest> = new EventEmitter<UpdateComponentRequest>();

    editLabelForm: FormGroup;
    readonly: boolean;

    fontSizeOptions: SelectOption[] = [12, 14, 16, 18, 20, 22, 24].map((fontSize) => {
        return {
            text: `${fontSize}px`,
            value: `${fontSize}px`
        };
    });

    constructor() {
        super();
        const request = this.request;

        this.readonly = !request.entity.permissions.canWrite;

        let fontSize = this.fontSizeOptions[0].value;
        if (request.entity.component.style['font-size']) {
            fontSize = request.entity.component.style['font-size'];
        }

        this.editLabelForm = this.formBuilder.group({
            value: new FormControl(request.entity.component.label, Validators.required),
            fontSize: new FormControl(fontSize, Validators.required)
        });
    }

    submitForm() {
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

        this.editLabel.emit({
            id: this.request.entity.id,
            type: this.request.type,
            uri: this.request.uri,
            payload,
            errorStrategy: 'banner'
        });
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
