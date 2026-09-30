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

import { Component, inject, Input, OnInit, signal, WritableSignal } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { ImportFromRegistryDialogRequest } from '../../../../../state/flow';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../../../state';
import {
    BranchEntity,
    BucketEntity,
    RegistryClientEntity,
    VersionedFlow,
    VersionedFlowEntity,
    VersionedFlowSnapshotMetadata,
    VersionedFlowSnapshotMetadataEntity
} from '../../../../../../../state/shared';
import { selectSaving } from '../../../../../state/flow/flow.selectors';
import { AsyncPipe } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { catchError, EMPTY, finalize, map, Observable, of, Subject, switchMap, take } from 'rxjs';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';
import {
    CloseOnEscapeDialog,
    isDefinedAndNotNull,
    NiFiCommon,
    NifiSpinnerDirective,
    NifiTooltipDirective,
    SelectOption,
    TextTip
} from '@nifi/shared';
import { selectTimeOffset } from '../../../../../../../state/flow-configuration/flow-configuration.selectors';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { Client } from '../../../../../../../service/client.service';
import { importFromRegistry } from '../../../../../state/flow/flow.actions';
import { ClusterConnectionService } from '../../../../../../../service/cluster-connection.service';
import { ErrorContextKey } from '../../../../../../../state/error';
import { ContextErrorBanner } from '../../../../../../../ui/common/context-error-banner/context-error-banner.component';
import { ErrorHelper } from '../../../../../../../service/error-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { NgxSkeletonLoaderComponent } from 'ngx-skeleton-loader';

interface LoadBucketsRequest {
    registryId: string;
    branch: string | null;
}

interface LoadFlowsRequest extends LoadBucketsRequest {
    bucketId: string;
}

interface LoadVersionsRequest extends LoadFlowsRequest {
    flowId: string;
}

@Component({
    selector: 'import-from-registry',
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
        NifiTooltipDirective,
        MatIconModule,
        MatCheckboxModule,
        MatSortModule,
        MatTableModule,
        ContextErrorBanner,
        NgxSkeletonLoaderComponent
    ],
    templateUrl: './import-from-registry.component.html',
    styleUrls: ['./import-from-registry.component.scss']
})
export class ImportFromRegistry extends CloseOnEscapeDialog implements OnInit {
    private dialogRequest = inject<ImportFromRegistryDialogRequest>(MAT_DIALOG_DATA);
    private formBuilder = inject(FormBuilder);
    private store = inject<Store<CanvasState>>(Store);
    private nifiCommon = inject(NiFiCommon);
    private client = inject(Client);
    private clusterConnectionService = inject(ClusterConnectionService);
    private errorHelper = inject(ErrorHelper);

    @Input() getBranches: (registryId: string) => Observable<BranchEntity[]> = () => of([]);
    @Input() getBuckets!: (registryId: string, branch?: string | null) => Observable<BucketEntity[]>;
    @Input() getFlows!: (
        registryId: string,
        bucketId: string,
        branch?: string | null
    ) => Observable<VersionedFlowEntity[]>;
    @Input() getFlowVersions!: (
        registryId: string,
        bucketId: string,
        flowId: string,
        branch?: string | null
    ) => Observable<VersionedFlowSnapshotMetadataEntity[]>;

    saving$ = this.store.select(selectSaving);
    timeOffset = 0;

    protected readonly TextTip = TextTip;

    importFromRegistryForm: FormGroup;
    registryClientOptions: SelectOption[] = [];
    branchOptions: SelectOption[] = [];
    bucketOptions: SelectOption[] = [];
    flowOptions: SelectOption[] = [];

    flowLookup: Map<string, VersionedFlow> = new Map<string, VersionedFlow>();
    selectedFlowDescription: string | undefined;
    supportsBranching = false;
    private clientBranchingSupportMap: Map<string, boolean> = new Map<string, boolean>();

    sort: Sort = {
        active: 'created',
        direction: 'desc'
    };
    displayedColumns: string[] = ['version', 'created', 'comments'];
    dataSource: MatTableDataSource<VersionedFlowSnapshotMetadata> =
        new MatTableDataSource<VersionedFlowSnapshotMetadata>();
    selectedFlowVersion: string | null = null;

    loadingBranches: WritableSignal<boolean> = signal(false);
    loadingBuckets: WritableSignal<boolean> = signal(false);
    loadingFlows: WritableSignal<boolean> = signal(false);
    loadingVersions: WritableSignal<boolean> = signal(false);
    loadingVersionsError: WritableSignal<string | null> = signal(null);

    private loadBranchesRequest = new Subject<string | null>();
    private loadBucketsRequest = new Subject<LoadBucketsRequest | null>();
    private loadFlowsRequest = new Subject<LoadFlowsRequest | null>();
    private loadVersionsRequest = new Subject<LoadVersionsRequest | null>();

    constructor() {
        super();
        const dialogRequest = this.dialogRequest;

        this.store
            .select(selectTimeOffset)
            .pipe(isDefinedAndNotNull(), takeUntilDestroyed())
            .subscribe((timeOffset: number) => {
                this.timeOffset = timeOffset;
            });

        const sortedRegistries = dialogRequest.registryClients
            .slice()
            .filter((registry) => registry.permissions.canRead)
            .sort((a, b) => {
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

        const initialRegistry = this.registryClientOptions.length > 0 ? this.registryClientOptions[0].value : null;
        this.importFromRegistryForm = this.formBuilder.group({
            registry: new FormControl(initialRegistry, Validators.required),
            branch: new FormControl(null),
            bucket: new FormControl(null, Validators.required),
            flow: new FormControl(null, Validators.required),
            keepParameterContexts: new FormControl(true, Validators.required)
        });

        this.loadBranchesRequest
            .pipe(
                switchMap((registryId: string | null) => {
                    if (!registryId) {
                        return EMPTY;
                    }

                    this.setLoading('branch', true);
                    this.branchOptions = [];

                    return this.getBranches(registryId).pipe(
                        take(1),
                        map((branches: BranchEntity[]) => ({ registryId, branches })),
                        catchError(() => EMPTY),
                        finalize(() => this.setLoading('branch', false))
                    );
                }),
                takeUntilDestroyed()
            )
            .subscribe(({ registryId, branches }) => {
                branches.forEach((entity: BranchEntity) => {
                    this.branchOptions.push({
                        text: entity.branch.name,
                        value: entity.branch.name
                    });
                });

                const branch = this.branchOptions[0]?.value;
                if (branch) {
                    this.importFromRegistryForm.get('branch')?.setValue(branch);
                    this.loadBucketsRequest.next({ registryId, branch });
                }
            });

        this.loadBucketsRequest
            .pipe(
                switchMap((request: LoadBucketsRequest | null) => {
                    if (!request) {
                        return EMPTY;
                    }

                    this.setLoading('bucket', true);
                    this.bucketOptions = [];

                    return this.getBuckets(request.registryId, request.branch).pipe(
                        take(1),
                        map((buckets: BucketEntity[]) => ({ request, buckets })),
                        catchError(() => EMPTY),
                        finalize(() => this.setLoading('bucket', false))
                    );
                }),
                takeUntilDestroyed()
            )
            .subscribe(({ request, buckets }) => {
                buckets.forEach((entity: BucketEntity) => {
                    if (entity.permissions.canRead) {
                        this.bucketOptions.push({
                            text: entity.bucket.name,
                            value: entity.id,
                            description: entity.bucket.description
                        });
                    }
                });

                const bucketId = this.bucketOptions[0]?.value;
                if (bucketId) {
                    this.importFromRegistryForm.get('bucket')?.setValue(bucketId);
                    this.loadFlowsRequest.next({ ...request, bucketId });
                }
            });

        this.loadFlowsRequest
            .pipe(
                switchMap((request: LoadFlowsRequest | null) => {
                    if (!request) {
                        return EMPTY;
                    }

                    this.setLoading('flow', true);
                    this.flowOptions = [];
                    this.flowLookup.clear();

                    return this.getFlows(request.registryId, request.bucketId, request.branch).pipe(
                        take(1),
                        map((versionedFlows: VersionedFlowEntity[]) => ({ request, versionedFlows })),
                        catchError(() => EMPTY),
                        finalize(() => this.setLoading('flow', false))
                    );
                }),
                takeUntilDestroyed()
            )
            .subscribe(({ request, versionedFlows }) => {
                versionedFlows.forEach((entity: VersionedFlowEntity) => {
                    this.flowLookup.set(entity.versionedFlow.flowId!, entity.versionedFlow);

                    this.flowOptions.push({
                        text: entity.versionedFlow.flowName,
                        value: entity.versionedFlow.flowId!,
                        description: entity.versionedFlow.description
                    });
                });

                const flowId = this.flowOptions[0]?.value;
                if (flowId) {
                    this.importFromRegistryForm.get('flow')?.setValue(flowId);
                    this.loadVersionsRequest.next({ ...request, flowId });
                }
            });

        this.loadVersionsRequest
            .pipe(
                switchMap((request: LoadVersionsRequest | null) => {
                    if (!request) {
                        this.loadingVersionsError.set(null);
                        return EMPTY;
                    }

                    this.setLoading('version', true);
                    this.loadingVersionsError.set(null);
                    this.dataSource.data = [];
                    this.selectedFlowVersion = null;
                    this.selectedFlowDescription = this.flowLookup.get(request.flowId)?.description;

                    return this.getFlowVersions(
                        request.registryId,
                        request.bucketId,
                        request.flowId,
                        request.branch
                    ).pipe(
                        take(1),
                        catchError((errorResponse: HttpErrorResponse) => {
                            this.loadingVersionsError.set(this.errorHelper.getErrorString(errorResponse));
                            return EMPTY;
                        }),
                        finalize(() => this.setLoading('version', false))
                    );
                }),
                takeUntilDestroyed()
            )
            .subscribe((metadataEntities: VersionedFlowSnapshotMetadataEntity[]) => {
                if (metadataEntities.length > 0) {
                    const flowVersions = metadataEntities.map(
                        (entity: VersionedFlowSnapshotMetadataEntity) => entity.versionedFlowSnapshotMetadata
                    );

                    const sortedFlowVersions = this.sortVersions(flowVersions, this.sort);
                    this.selectedFlowVersion = sortedFlowVersions[0].version;

                    this.dataSource.data = sortedFlowVersions;
                }
            });
    }

    ngOnInit(): void {
        const selectedRegistryId = this.importFromRegistryForm.get('registry')?.value;

        if (selectedRegistryId) {
            this.registrySelected(selectedRegistryId);
        }
    }

    registryChanged(registryId: string): void {
        this.registrySelected(registryId);
    }

    private registrySelected(registryId: string): void {
        this.supportsBranching = this.clientBranchingSupportMap.get(registryId) || false;
        this.clearBranches();
        if (this.supportsBranching) {
            this.loadBranchesRequest.next(registryId);
        } else {
            this.loadBucketsRequest.next({ registryId, branch: null });
        }
    }

    private clearBranches(): void {
        this.branchOptions = [];
        this.importFromRegistryForm.get('branch')?.setValue(null);
        this.loadBranchesRequest.next(null);
        this.clearBuckets();
    }

    branchChanged(branch: string): void {
        this.clearBuckets();
        const registryId = this.importFromRegistryForm.get('registry')?.value;
        this.loadBucketsRequest.next({ registryId, branch });
    }

    private clearBuckets(): void {
        this.bucketOptions = [];
        this.importFromRegistryForm.get('bucket')?.setValue(null);
        this.loadBucketsRequest.next(null);
        this.clearFlows();
    }

    bucketChanged(bucketId: string): void {
        this.clearFlows();
        const registryId = this.importFromRegistryForm.get('registry')?.value;
        const branch = this.importFromRegistryForm.get('branch')?.value;
        this.loadFlowsRequest.next({ registryId, branch, bucketId });
    }

    private clearFlows() {
        this.importFromRegistryForm.get('flow')?.setValue(null);
        this.flowOptions = [];
        this.dataSource.data = [];
        this.selectedFlowVersion = null;
        this.loadingVersionsError.set(null);
        this.loadFlowsRequest.next(null);
        this.loadVersionsRequest.next(null);
    }

    flowChanged(flowId: string): void {
        const registryId = this.importFromRegistryForm.get('registry')?.value;
        const bucketId = this.importFromRegistryForm.get('bucket')?.value;
        const branch = this.importFromRegistryForm.get('branch')?.value;
        this.loadVersionsRequest.next({ registryId, branch, bucketId, flowId });
    }

    private setLoading(resourceType: string, loading: boolean): void {
        let formControl;
        switch (resourceType) {
            case 'branch':
                this.loadingBranches.set(loading);
                formControl = this.importFromRegistryForm.get('branch');
                break;
            case 'bucket':
                this.loadingBuckets.set(loading);
                formControl = this.importFromRegistryForm.get('bucket');
                break;
            case 'flow':
                this.loadingFlows.set(loading);
                formControl = this.importFromRegistryForm.get('flow');
                break;
            case 'version':
                this.loadingVersions.set(loading);
                break;
        }
        if (formControl) {
            if (loading) {
                formControl.disable();
            } else {
                formControl.enable();
            }
        }
    }

    formatTimestamp(flowVersion: VersionedFlowSnapshotMetadata) {
        // get the current user time to properly convert the server time
        const now: Date = new Date();

        // convert the user offset to millis
        const userTimeOffset: number = now.getTimezoneOffset() * 60 * 1000;

        // create the proper date by adjusting by the offsets
        const date: Date = new Date(flowVersion.timestamp + userTimeOffset + this.timeOffset);
        return this.nifiCommon.formatDateTime(date);
    }

    sortData(sort: Sort) {
        this.sort = sort;
        this.dataSource.data = this.sortVersions(this.dataSource.data, sort);
    }

    sortVersions(data: VersionedFlowSnapshotMetadata[], sort: Sort): VersionedFlowSnapshotMetadata[] {
        if (!data) {
            return [];
        }
        return data.slice().sort((a, b) => {
            const isAsc = sort.direction === 'asc';
            let retVal = 0;
            switch (sort.active) {
                case 'version':
                    retVal = this.compareVersion(a.version, b.version);
                    break;
                case 'created':
                    retVal = this.nifiCommon.compareNumber(a.timestamp, b.timestamp);
                    break;
                case 'comments':
                    retVal = this.nifiCommon.compareString(a.comments, b.comments);
                    break;
            }
            return retVal * (isAsc ? 1 : -1);
        });
    }

    private compareVersion(a: string, b: string): number {
        if (this.nifiCommon.isNumber(a) && this.nifiCommon.isNumber(b)) {
            return this.nifiCommon.compareNumber(parseInt(a, 10), parseInt(b, 10));
        } else {
            return this.nifiCommon.compareString(a, b);
        }
    }

    select(flowVersion: VersionedFlowSnapshotMetadata): void {
        this.selectedFlowVersion = flowVersion.version;
    }

    isSelected(flowVersion: VersionedFlowSnapshotMetadata): boolean {
        if (this.selectedFlowVersion) {
            return flowVersion.version == this.selectedFlowVersion;
        }
        return false;
    }

    importFromRegistry(): void {
        if (this.selectedFlowVersion != null) {
            const payload: any = {
                revision: this.client.getRevision({
                    revision: {
                        version: 0
                    }
                }),
                disconnectedNodeAcknowledged: this.clusterConnectionService.isDisconnectionAcknowledged(),
                component: {
                    position: {
                        x: this.dialogRequest.request.position.x,
                        y: this.dialogRequest.request.position.y
                    },
                    versionControlInformation: {
                        registryId: this.importFromRegistryForm.get('registry')?.value,
                        bucketId: this.importFromRegistryForm.get('bucket')?.value,
                        flowId: this.importFromRegistryForm.get('flow')?.value,
                        version: this.selectedFlowVersion
                    }
                }
            };

            if (this.supportsBranching) {
                payload.component.versionControlInformation.branch = this.importFromRegistryForm.get('branch')?.value;
            }

            this.store.dispatch(
                importFromRegistry({
                    request: {
                        payload,
                        keepExistingParameterContext: this.importFromRegistryForm.get('keepParameterContexts')?.value
                    }
                })
            );
        }
    }

    override isDirty(): boolean {
        return this.importFromRegistryForm.dirty;
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
