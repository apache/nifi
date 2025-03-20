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

import { Component, Inject, Input } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import {
    EditConnectionDialogRequest,
    loadBalanceCompressionStrategies,
    loadBalanceStrategies
} from '../../../../../state/flow';
import { Store } from '@ngrx/store';
import { selectBreadcrumbs, selectSaving } from '../../../../../state/flow/flow.selectors';
import { AsyncPipe } from '@angular/common';
import { FormBuilder, FormControl, FormGroup, FormsModule, ReactiveFormsModule, Validators } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { NifiSpinnerDirective } from '../../../../../../../ui/common/spinner/nifi-spinner.directive';
import { ComponentType, CopyDirective, NifiTooltipDirective, TextTip } from '@nifi/shared';
import { MatTabsModule } from '@angular/material/tabs';
import { NiFiState } from '../../../../../../../state';
import { selectPrioritizerTypes } from '../../../../../../../state/extension-types/extension-types.selectors';
import { Prioritizers } from '../prioritizers/prioritizers.component';
import { SourceProcessor } from '../source/source-processor/source-processor.component';
import { DestinationFunnel } from '../destination/destination-funnel/destination-funnel.component';
import { updateConnection } from '../../../../../state/flow/flow.actions';
import { Client } from '../../../../../../../service/client.service';
import { CanvasUtils } from '../../../../../service/canvas-utils.service';
import { SourceFunnel } from '../source/source-funnel/source-funnel.component';
import { DestinationProcessor } from '../destination/destination-processor/destination-processor.component';
import { DestinationOutputPort } from '../destination/destination-output-port/destination-output-port.component';
import { SourceInputPort } from '../source/source-input-port/source-input-port.component';
import { asyncScheduler, Observable, observeOn, tap } from 'rxjs';
import { SourceProcessGroup } from '../source/source-process-group/source-process-group.component';
import { DestinationProcessGroup } from '../destination/destination-process-group/destination-process-group.component';
import { SourceRemoteProcessGroup } from '../source/source-remote-process-group/source-remote-process-group.component';
import { DestinationRemoteProcessGroup } from '../destination/destination-remote-process-group/destination-remote-process-group.component';
import { BreadcrumbEntity } from '../../../../../state/shared';
import { TabbedDialog } from '../../../../../../../ui/common/tabbed-dialog/tabbed-dialog.component';
import { ErrorContextKey } from '../../../../../../../state/error';
import { ContextErrorBanner } from '../../../../../../../ui/common/context-error-banner/context-error-banner.component';

@Component({
    selector: 'edit-connection',
    imports: [
        AsyncPipe,
        FormsModule,
        MatButtonModule,
        MatDialogModule,
        MatFormFieldModule,
        MatInputModule,
        MatOptionModule,
        MatSelectModule,
        NifiSpinnerDirective,
        NifiTooltipDirective,
        ReactiveFormsModule,
        MatTabsModule,
        Prioritizers,
        SourceProcessor,
        DestinationFunnel,
        SourceFunnel,
        DestinationProcessor,
        DestinationOutputPort,
        SourceInputPort,
        SourceProcessGroup,
        DestinationProcessGroup,
        SourceRemoteProcessGroup,
        DestinationRemoteProcessGroup,
        ContextErrorBanner,
        CopyDirective
    ],
    templateUrl: './edit-connection.component.html',
    styleUrls: ['./edit-connection.component.scss']
})
export class EditConnectionComponent extends TabbedDialog {
    @Input() set getChildOutputPorts(getChildOutputPorts: (groupId: string) => Observable<any>) {
        if (this.sourceType == ComponentType.ProcessGroup) {
            this.childOutputPorts$ = getChildOutputPorts(this.source.groupId);
            this.sourceReadonly = false;
            this.updateControlValueAccessorsForReadOnly();
        }
    }

    @Input() set getChildInputPorts(getChildInputPorts: (groupId: string) => Observable<any>) {
        if (this.destinationType == ComponentType.ProcessGroup) {
            this.childInputPorts$ = getChildInputPorts(this.destinationGroupId);
            this.destinationReadonly = false;
            this.updateControlValueAccessorsForReadOnly();
        }
    }

    @Input() set selectProcessor(selectProcessor: (id: string) => Observable<any>) {
        if (this.sourceType == ComponentType.Processor) {
            this.sourceProcessor$ = selectProcessor(this.source.id).pipe(
                observeOn(asyncScheduler),
                tap((processor) => {
                    this.sourceReadonly = !this.canvasUtils.runnableSupportsModification(processor);
                    this.updateControlValueAccessorsForReadOnly();
                })
            );
        }
        if (this.destinationType == ComponentType.Processor && this.destinationId) {
            this.destinationProcessor$ = selectProcessor(this.destinationId).pipe(
                observeOn(asyncScheduler),
                tap((processor) => {
                    this.destinationReadonly = !this.canvasUtils.runnableSupportsModification(processor);
                    this.updateControlValueAccessorsForReadOnly();
                })
            );
        }
    }

    @Input() set selectInputPort(selectInputPort: (id: string) => Observable<any>) {
        if (this.sourceType == ComponentType.InputPort) {
            this.sourceInputPort$ = selectInputPort(this.source.id).pipe(
                observeOn(asyncScheduler),
                tap((inputPort) => {
                    this.sourceReadonly = !this.canvasUtils.runnableSupportsModification(inputPort);
                    this.updateControlValueAccessorsForReadOnly();
                })
            );
        }
    }

    @Input() set selectOutputPort(selectOutputPort: (id: string) => Observable<any>) {
        if (this.destinationType == ComponentType.OutputPort && this.destinationId) {
            this.destinationOutputPort$ = selectOutputPort(this.destinationId).pipe(
                observeOn(asyncScheduler),
                tap((outputPort) => {
                    this.destinationReadonly = !this.canvasUtils.runnableSupportsModification(outputPort);
                    this.updateControlValueAccessorsForReadOnly();
                })
            );
        }
    }

    @Input() set selectProcessGroup(selectProcessGroup: (id: string) => Observable<any>) {
        if (this.sourceType == ComponentType.ProcessGroup) {
            this.sourceProcessGroup$ = selectProcessGroup(this.source.groupId);
            this.sourceReadonly = false;
            this.updateControlValueAccessorsForReadOnly();
        }
        if (this.destinationType == ComponentType.ProcessGroup) {
            this.destinationProcessGroup$ = selectProcessGroup(this.destinationGroupId);
            this.destinationReadonly = false;
            this.updateControlValueAccessorsForReadOnly();
        }
    }

    @Input() set selectRemoteProcessGroup(selectRemoteProcessGroup: (id: string) => Observable<any>) {
        if (this.sourceType == ComponentType.RemoteProcessGroup) {
            this.sourceRemoteProcessGroup$ = selectRemoteProcessGroup(this.source.groupId).pipe(
                observeOn(asyncScheduler),
                tap((remoteProcessGroup) => {
                    this.sourceReadonly = !this.canvasUtils.remoteProcessGroupSupportsModification(remoteProcessGroup);
                    this.updateControlValueAccessorsForReadOnly();
                })
            );
        }
        if (this.destinationType == ComponentType.RemoteProcessGroup) {
            this.destinationRemoteProcessGroup$ = selectRemoteProcessGroup(this.destinationGroupId).pipe(
                observeOn(asyncScheduler),
                tap((remoteProcessGroup) => {
                    this.destinationReadonly =
                        !this.canvasUtils.remoteProcessGroupSupportsModification(remoteProcessGroup);
                    this.updateControlValueAccessorsForReadOnly();
                })
            );
        }
    }

    protected readonly ComponentType = ComponentType;
    protected readonly TextTip = TextTip;

    saving$ = this.store.select(selectSaving);
    availablePrioritizers$ = this.store.select(selectPrioritizerTypes);
    breadcrumbs$ = this.store.select(selectBreadcrumbs);

    editConnectionForm: FormGroup;
    connectionReadonly: boolean;
    sourceReadonly: boolean = false;
    destinationReadonly: boolean = false;

    source: any;
    sourceType: ComponentType | null;

    previousDestination: any;

    destinationType: ComponentType | null;
    destinationId: string | null = null;
    destinationGroupId: string;
    destinationName: string;

    sourceProcessor$: Observable<any> | null = null;
    destinationProcessor$: Observable<any> | null = null;
    sourceInputPort$: Observable<any> | null = null;
    destinationOutputPort$: Observable<any> | null = null;
    sourceProcessGroup$: Observable<any> | null = null;
    destinationProcessGroup$: Observable<any> | null = null;
    sourceRemoteProcessGroup$: Observable<any> | null = null;
    destinationRemoteProcessGroup$: Observable<any> | null = null;
    childOutputPorts$: Observable<any> | null = null;
    childInputPorts$: Observable<any> | null = null;

    loadBalancePartitionAttributeRequired = false;
    initialPartitionAttribute: string;
    loadBalanceCompressionRequired = false;
    initialCompression: string;

    constructor(
        @Inject(MAT_DIALOG_DATA) public dialogRequest: EditConnectionDialogRequest,
        private formBuilder: FormBuilder,
        private store: Store<NiFiState>,
        private canvasUtils: CanvasUtils,
        private client: Client
    ) {
        super('edit-connection-selected-index');

        const connection: any = dialogRequest.entity.component;

        this.connectionReadonly = !dialogRequest.entity.permissions.canWrite;

        this.source = connection.source;
        this.sourceType = this.canvasUtils.getComponentTypeForSource(this.source.type);

        // set the destination details accordingly so the proper component can be loaded.
        // the user may be editing a connection or changing the destination
        if (dialogRequest.newDestination) {
            const newDestination: any = dialogRequest.newDestination;
            this.destinationType = newDestination.type;
            this.destinationGroupId = newDestination.groupId;
            this.destinationName = newDestination.name;
            if (newDestination.id) {
                this.destinationId = newDestination.id;
            }

            this.previousDestination = connection.destination;
        } else {
            this.destinationType = this.canvasUtils.getComponentTypeForDestination(connection.destination.type);
            this.destinationGroupId = connection.destination.groupId;
            this.destinationId = connection.destination.id;
            this.destinationName = connection.destination.name;
        }

        this.editConnectionForm = this.formBuilder.group({
            name: new FormControl(connection.name),
            flowFileExpiration: new FormControl(connection.flowFileExpiration, Validators.required),
            backPressureObjectThreshold: new FormControl(connection.backPressureObjectThreshold, [
                Validators.required,
                Validators.min(0)
            ]),
            backPressureDataSizeThreshold: new FormControl(
                connection.backPressureDataSizeThreshold,
                Validators.required
            ),
            loadBalanceStrategy: new FormControl(connection.loadBalanceStrategy, Validators.required),
            prioritizers: new FormControl(connection.prioritizers)
        });

        if (this.sourceType == ComponentType.Processor) {
            this.editConnectionForm.addControl(
                'relationships',
                new FormControl(connection.selectedRelationships, Validators.required)
            );
        }

        if (this.sourceType == ComponentType.ProcessGroup || this.sourceType == ComponentType.RemoteProcessGroup) {
            this.editConnectionForm.addControl(
                'source',
                new FormControl({ value: this.source.id, disabled: true }, Validators.required)
            );
        }

        if (
            this.destinationType == ComponentType.ProcessGroup ||
            this.destinationType == ComponentType.RemoteProcessGroup
        ) {
            this.editConnectionForm.addControl('destination', new FormControl(this.destinationId, Validators.required));
        }

        this.initialPartitionAttribute = connection.loadBalancePartitionAttribute;
        this.initialCompression = connection.loadBalanceCompression;
        this.loadBalanceChanged(connection.loadBalanceStrategy);

        this.updateControlValueAccessorsForReadOnly();
    }

    updateControlValueAccessorsForReadOnly(): void {
        const disabled = this.connectionReadonly || this.sourceReadonly || this.destinationReadonly;

        // sourceReadonly is used to update the readonly / disable state of the form controls, note that
        // the source control for local and remote groups is always disabled (see above) in this edit
        // component because the source of the connection cannot be changed

        if (disabled) {
            this.editConnectionForm.get('prioritizers')?.disable();

            if (this.sourceType == ComponentType.Processor) {
                this.editConnectionForm.get('relationships')?.disable();
            }

            if (
                this.destinationType == ComponentType.ProcessGroup ||
                this.destinationType == ComponentType.RemoteProcessGroup
            ) {
                this.editConnectionForm.get('destination')?.disable();
            }
        } else {
            this.editConnectionForm.get('prioritizers')?.enable();

            if (this.sourceType == ComponentType.Processor) {
                this.editConnectionForm.get('relationships')?.enable();
            }

            if (
                this.destinationType == ComponentType.ProcessGroup ||
                this.destinationType == ComponentType.RemoteProcessGroup
            ) {
                this.editConnectionForm.get('destination')?.enable();
            }
        }
    }

    getCurrentGroupName(breadcrumbs: BreadcrumbEntity): string {
        if (breadcrumbs.permissions.canRead) {
            return breadcrumbs.breadcrumb.name;
        } else {
            return breadcrumbs.id;
        }
    }

    loadBalanceChanged(value: string): void {
        if (value == 'PARTITION_BY_ATTRIBUTE') {
            this.editConnectionForm.addControl(
                'partitionAttribute',
                new FormControl(this.initialPartitionAttribute, Validators.required)
            );
            this.loadBalancePartitionAttributeRequired = true;
        } else {
            this.editConnectionForm.removeControl('partitionAttribute');
            this.loadBalancePartitionAttributeRequired = false;
        }

        if (value == 'DO_NOT_LOAD_BALANCE') {
            this.loadBalanceCompressionRequired = false;
            this.editConnectionForm.removeControl('compression');
        } else {
            this.loadBalanceCompressionRequired = true;
            this.editConnectionForm.addControl(
                'compression',
                new FormControl(this.initialCompression, Validators.required)
            );
        }
    }

    editConnection(): void {
        const d: any = this.dialogRequest.entity;

        const payload: any = {
            revision: this.client.getRevision(d),
            component: {
                id: d.id,
                backPressureDataSizeThreshold: this.editConnectionForm.get('backPressureDataSizeThreshold')?.value,
                backPressureObjectThreshold: this.editConnectionForm.get('backPressureObjectThreshold')?.value,
                flowFileExpiration: this.editConnectionForm.get('flowFileExpiration')?.value,
                loadBalanceStrategy: this.editConnectionForm.get('loadBalanceStrategy')?.value,
                name: this.editConnectionForm.get('name')?.value,
                prioritizers: this.editConnectionForm.get('prioritizers')?.value
            }
        };

        if (this.sourceType == ComponentType.Processor) {
            payload.component.selectedRelationships = this.editConnectionForm.get('relationships')?.value;
        }

        if (
            this.destinationType == ComponentType.ProcessGroup ||
            this.destinationType == ComponentType.RemoteProcessGroup
        ) {
            payload.component.destination = {
                groupId: this.destinationGroupId,
                id: this.editConnectionForm.get('destination')?.value,
                type: this.canvasUtils.getConnectableTypeForDestination(this.destinationType)
            };
        }

        if (this.loadBalancePartitionAttributeRequired) {
            payload.component.loadBalancePartitionAttribute = this.editConnectionForm.get('partitionAttribute')?.value;
        } else {
            payload.component.loadBalancePartitionAttribute = '';
        }

        if (this.loadBalanceCompressionRequired) {
            payload.component.loadBalanceCompression = this.editConnectionForm.get('compression')?.value;
        } else {
            payload.component.loadBalanceCompression = 'DO_NOT_COMPRESS';
        }

        this.store.dispatch(
            updateConnection({
                request: {
                    id: this.dialogRequest.entity.id,
                    type: ComponentType.Connection,
                    uri: this.dialogRequest.entity.uri,
                    previousDestination: this.previousDestination,
                    payload,
                    errorStrategy: 'banner'
                }
            })
        );
    }

    protected readonly loadBalanceStrategies = loadBalanceStrategies;
    protected readonly loadBalanceCompressionStrategies = loadBalanceCompressionStrategies;

    override isDirty(): boolean {
        return this.editConnectionForm.dirty;
    }

    override getCancelDialogResult(): any {
        return 'CANCELLED';
    }

    protected readonly ErrorContextKey = ErrorContextKey;
}
