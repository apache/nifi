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
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../../state';
import { createRemoteProcessGroup } from '../../../../../state/flow/flow.actions';
import { selectSaving } from '../../../../../state/flow/flow.selectors';
import { AsyncPipe } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { CreateComponentRequest } from '../../../../../state/flow';
import { NifiTooltipDirective, TextTip } from '@nifi/shared';
import { CloseOnEscapeDialog } from '@nifi/shared';
import { ErrorContextKey } from '../../../../../../../state/error';
import { ContextErrorBanner } from '../../../../../../../ui/common/context-error-banner/context-error-banner.component';

@Component({
    imports: [
        AsyncPipe,
        MatButtonModule,
        MatDialogModule,
        MatFormFieldModule,
        MatInputModule,
        NifiSpinnerDirective,
        ReactiveFormsModule,
        MatOptionModule,
        MatSelectModule,
        MatIconModule,
        NifiTooltipDirective,
        ContextErrorBanner
    ],
    templateUrl: './create-remote-process-group.component.html',
    styleUrls: ['./create-remote-process-group.component.scss']
})
export class CreateRemoteProcessGroup extends CloseOnEscapeDialog {
    saving$ = this.store.select(selectSaving);

    createRemoteProcessGroupForm: FormGroup;

    constructor(
        @Inject(MAT_DIALOG_DATA) private dialogRequest: CreateComponentRequest,
        private formBuilder: FormBuilder,
        private store: Store<CanvasState>
    ) {
        super();
        this.createRemoteProcessGroupForm = this.formBuilder.group({
            urls: new FormControl('', Validators.required),
            transportProtocol: new FormControl('RAW', Validators.required),
            localNetworkInterface: new FormControl(''),
            httpProxyServerHostname: new FormControl(''),
            httpProxyServerPort: new FormControl(''),
            httpProxyUser: new FormControl(''),
            httpProxyPassword: new FormControl(''),
            communicationsTimeout: new FormControl('30 sec', Validators.required),
            yieldDuration: new FormControl('10 sec', Validators.required)
        });
    }

    createRemoteProcessGroup(): void {
        this.store.dispatch(
            createRemoteProcessGroup({
                request: {
                    ...this.dialogRequest,
                    targetUris: this.createRemoteProcessGroupForm.get('urls')?.value,
                    transportProtocol: this.createRemoteProcessGroupForm.get('transportProtocol')?.value,
                    localNetworkInterface: this.createRemoteProcessGroupForm.get('localNetworkInterface')?.value,
                    proxyHost: this.createRemoteProcessGroupForm.get('httpProxyServerHostname')?.value,
                    proxyPort: this.createRemoteProcessGroupForm.get('httpProxyServerPort')?.value,
                    proxyUser: this.createRemoteProcessGroupForm.get('httpProxyUser')?.value,
                    proxyPassword: this.createRemoteProcessGroupForm.get('httpProxyPassword')?.value,
                    communicationsTimeout: this.createRemoteProcessGroupForm.get('communicationsTimeout')?.value,
                    yieldDuration: this.createRemoteProcessGroupForm.get('yieldDuration')?.value
                }
            })
        );
    }

    protected readonly TextTip = TextTip;
    protected readonly ErrorContextKey = ErrorContextKey;
}
