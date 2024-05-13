/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, EventEmitter, Inject, Input, OnInit, Output, Signal } from '@angular/core';
import {
    MAT_DIALOG_DATA,
    MatDialogActions,
    MatDialogClose,
    MatDialogContent,
    MatDialogTitle
} from '@angular/material/dialog';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { ErrorBanner } from '../../../../../../../ui/common/error-banner/error-banner.component';
import { MatButton } from '@angular/material/button';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import { MatError, MatFormField, MatLabel } from '@angular/material/form-field';
import { MatOption, MatSelect } from '@angular/material/select';
import { Observable, of, take } from 'rxjs';
import { BranchEntity, BucketEntity, RegistryClientEntity, SelectOption } from '../../../../../../../state/shared';
import { NiFiCommon } from '../../../../../../../service/nifi-common.service';
import { SaveVersionDialogRequest, SaveVersionRequest, VersionControlInformation } from '../../../../../state/flow';
import { TextTip } from '../../../../../../../ui/common/tooltips/text-tip/text-tip.component';
import { NifiTooltipDirective } from '../../../../../../../ui/common/tooltips/nifi-tooltip.directive';
import { NgForOf, NgIf } from '@angular/common';
import { MatInput } from '@angular/material/input';

@Component({
    selector: 'save-version-dialog',
    standalone: true,
    imports: [
        MatDialogTitle,
        ReactiveFormsModule,
        ErrorBanner,
        MatDialogContent,
        MatDialogActions,
        MatButton,
        MatDialogClose,
        NifiSpinnerDirective,
        MatFormField,
        MatSelect,
        MatOption,
        NifiTooltipDirective,
        MatError,
        MatLabel,
        NgForOf,
        NgIf,
        MatInput
    ],
    templateUrl: './save-version-dialog.component.html',
    styleUrl: './save-version-dialog.component.scss'
})
export class SaveVersionDialog implements OnInit {
    @Input() getBranches: (registryId: string) => Observable<BranchEntity[]> = () => of([]);
    @Input() getBuckets: (registryId: string, branch?: string) => Observable<BucketEntity[]> = () => of([]);
    @Input({ required: true }) saving!: Signal<boolean>;

    @Output() save: EventEmitter<SaveVersionRequest> = new EventEmitter<SaveVersionRequest>();

    saveVersionForm: FormGroup;
    registryClientOptions: SelectOption[] = [];
    branchOptions: SelectOption[] = [];
    bucketOptions: SelectOption[] = [];
    versionControlInformation?: VersionControlInformation;
    forceCommit = false;
    supportsBranching = false;

    private clientBranchingSupportMap: Map<string, boolean> = new Map<string, boolean>();

    constructor(
        @Inject(MAT_DIALOG_DATA) private dialogRequest: SaveVersionDialogRequest,
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        this.versionControlInformation = dialogRequest.versionControlInformation;
        this.forceCommit = !!dialogRequest.forceCommit;

        if (dialogRequest.registryClients) {
            const sortedRegistries = dialogRequest.registryClients.slice().sort((a, b) => {
                return this.nifiCommon.compareString(a.component.name, b.component.name);
            });

            sortedRegistries.forEach((registryClient: RegistryClientEntity) => {
                if (registryClient.permissions.canRead) {
                    this.registryClientOptions.push({
                        text: registryClient.component.name,
                        value: registryClient.id,
                        description: registryClient.component.description
                    });
                }
                this.clientBranchingSupportMap.set(registryClient.id, registryClient.component.supportsBranching);
            });

            this.saveVersionForm = formBuilder.group({
                registry: new FormControl(this.registryClientOptions[0].value, Validators.required),
                branch: new FormControl('default', Validators.required),
                bucket: new FormControl(null, Validators.required),
                flowName: new FormControl(null, Validators.required),
                flowDescription: new FormControl(null),
                comments: new FormControl(null)
            });
        } else {
            this.saveVersionForm = formBuilder.group({
                comments: new FormControl('')
            });
        }
    }

    ngOnInit(): void {
        if (this.dialogRequest.registryClients) {
            const selectedRegistryId: string | null = this.saveVersionForm.get('registry')?.value;

            if (selectedRegistryId) {
                this.supportsBranching = this.clientBranchingSupportMap.get(selectedRegistryId) || false;
                if (this.supportsBranching) {
                    this.loadBranches(selectedRegistryId);
                } else {
                    this.loadBuckets(selectedRegistryId);
                }
            }
        }
    }

    loadBranches(registryId: string): void {
        if (registryId) {
            this.branchOptions = [];

            this.getBranches(registryId)
                .pipe(take(1))
                .subscribe((branches: BranchEntity[]) => {
                    if (branches.length > 0) {
                        branches.forEach((entity: BranchEntity) => {
                            this.branchOptions.push({
                                text: entity.branch.name,
                                value: entity.branch.name
                            });
                        });

                        const branchId = this.branchOptions[0].value;
                        if (branchId) {
                            this.saveVersionForm.get('branch')?.setValue(branchId);
                            this.loadBuckets(registryId, branchId);
                        }
                    }
                });
        }
    }

    loadBuckets(registryId: string, branch?: string): void {
        if (registryId) {
            this.bucketOptions = [];

            this.getBuckets(registryId, branch)
                .pipe(take(1))
                .subscribe((buckets: BucketEntity[]) => {
                    if (buckets.length > 0) {
                        buckets.forEach((entity: BucketEntity) => {
                            // only allow buckets to be selectable if the user can read and write to them
                            if (entity.permissions.canRead && entity.permissions.canWrite) {
                                this.bucketOptions.push({
                                    text: entity.bucket.name,
                                    value: entity.id,
                                    description: entity.bucket.description
                                });
                            }
                        });

                        const bucketId = this.bucketOptions[0].value;
                        if (bucketId) {
                            this.saveVersionForm.get('bucket')?.setValue(bucketId);
                        }
                    }
                });
        }
    }

    registryChanged(registryId: string): void {
        this.supportsBranching = this.clientBranchingSupportMap.get(registryId) || false;
        if (this.supportsBranching) {
            this.loadBranches(registryId);
        } else {
            this.loadBuckets(registryId);
        }
    }

    branchChanged(branch: string): void {
        const registryId = this.saveVersionForm.get('registry')?.value;
        this.loadBuckets(registryId, branch);
    }

    submitForm() {
        let request: SaveVersionRequest;
        const vci = this.versionControlInformation;
        if (vci) {
            request = {
                existingFlowId: vci.flowId,
                processGroupId: this.dialogRequest.processGroupId,
                revision: this.dialogRequest.revision,
                registry: vci.registryId,
                bucket: vci.bucketId,
                comments: this.saveVersionForm.get('comments')?.value,
                flowDescription: vci.flowDescription,
                flowName: vci.flowName,
                branch: vci.branch
            };
        } else {
            request = {
                processGroupId: this.dialogRequest.processGroupId,
                revision: this.dialogRequest.revision,
                registry: this.saveVersionForm.get('registry')?.value,
                bucket: this.saveVersionForm.get('bucket')?.value,
                comments: this.saveVersionForm.get('comments')?.value,
                flowDescription: this.saveVersionForm.get('flowDescription')?.value,
                flowName: this.saveVersionForm.get('flowName')?.value
            };
            if (this.supportsBranching) {
                request.branch = this.saveVersionForm.get('branch')?.value;
            }
        }
        this.save.next(request);
    }

    protected readonly TextTip = TextTip;
}
