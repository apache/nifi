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

import { Component, EventEmitter, Inject, Input, Output } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatButtonModule } from '@angular/material/button';
import { AsyncPipe } from '@angular/common';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { Observable } from 'rxjs';
import { Client } from '../../../../../../../service/client.service';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import { TextTip } from '../../../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { EditComponentDialogRequest } from '../../../../../state/flow';
import { ErrorBanner } from '../../../../../../../ui/common/error-banner/error-banner.component';
import { CanvasUtils } from '../../../../../service/canvas-utils.service';
import { NifiTooltipDirective } from '../../../../../../../ui/common/tooltips/nifi-tooltip.directive';

@Component({
    standalone: true,
    templateUrl: './edit-remote-process-group.component.html',
    imports: [
        ReactiveFormsModule,
        MatDialogModule,
        MatInputModule,
        MatCheckboxModule,
        MatButtonModule,
        MatOptionModule,
        MatSelectModule,
        AsyncPipe,
        NifiSpinnerDirective,
        FormsModule,
        ErrorBanner,
        NifiTooltipDirective
    ],
    styleUrls: ['./edit-remote-process-group.component.scss']
})
export class EditRemoteProcessGroup {
    @Input() saving$!: Observable<boolean>;
    @Output() editRemoteProcessGroup: EventEmitter<any> = new EventEmitter<any>();

    protected readonly TextTip = TextTip;

    editRemoteProcessGroupForm: FormGroup;
    readonly: boolean;

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: EditComponentDialogRequest,
        private formBuilder: FormBuilder,
        private canvasUtils: CanvasUtils,
        private client: Client
    ) {
        this.readonly =
            !request.entity.permissions.canWrite ||
            !this.canvasUtils.remoteProcessGroupSupportsModification(request.entity);

        this.editRemoteProcessGroupForm = this.formBuilder.group({
            urls: new FormControl(request.entity.component.targetUris, Validators.required),
            transportProtocol: new FormControl(request.entity.component.transportProtocol, Validators.required),
            localNetworkInterface: new FormControl(request.entity.component.localNetworkInterface),
            httpProxyServerHostname: new FormControl(request.entity.component.httpProxyServerHostname),
            httpProxyServerPort: new FormControl(request.entity.component.httpProxyServerPort),
            httpProxyUser: new FormControl(request.entity.component.httpProxyUser),
            httpProxyPassword: new FormControl(request.entity.component.httpProxyPassword),
            communicationsTimeout: new FormControl(request.entity.component.communicationsTimeout, Validators.required),
            yieldDuration: new FormControl(request.entity.component.yieldDuration, Validators.required)
        });
    }

    submitForm() {
        const payload: any = {
            revision: this.client.getRevision(this.request.entity),
            component: {
                id: this.request.entity.id,
                targetUris: this.editRemoteProcessGroupForm.get('urls')?.value,
                transportProtocol: this.editRemoteProcessGroupForm.get('transportProtocol')?.value,
                localNetworkInterface: this.editRemoteProcessGroupForm.get('localNetworkInterface')?.value,
                proxyHost: this.editRemoteProcessGroupForm.get('httpProxyServerHostname')?.value,
                proxyPort: this.editRemoteProcessGroupForm.get('httpProxyServerPort')?.value,
                proxyUser: this.editRemoteProcessGroupForm.get('httpProxyUser')?.value,
                proxyPassword: this.editRemoteProcessGroupForm.get('httpProxyPassword')?.value,
                communicationsTimeout: this.editRemoteProcessGroupForm.get('communicationsTimeout')?.value,
                yieldDuration: this.editRemoteProcessGroupForm.get('yieldDuration')?.value
            }
        };

        this.editRemoteProcessGroup.next(payload);
    }
}
