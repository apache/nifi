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

import {
    AfterViewInit,
    Component,
    DestroyRef,
    ElementRef,
    EventEmitter,
    inject,
    Input,
    Output,
    ViewChild
} from '@angular/core';

import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatOptionModule } from '@angular/material/core';
import { MatSelectModule } from '@angular/material/select';
import { FormBuilder, FormGroup, ReactiveFormsModule } from '@angular/forms';
import { NiFiCommon } from '../../../../../service/nifi-common.service';
import { BulletinBoardEvent, BulletinBoardFilterArgs, BulletinBoardItem } from '../../../state/bulletin-board';
import { BulletinEntity, ComponentType } from '../../../../../state/shared';
import { debounceTime, delay, Subject } from 'rxjs';
import { RouterLink } from '@angular/router';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
    selector: 'bulletin-board-list',
    standalone: true,
    imports: [MatFormFieldModule, MatInputModule, MatOptionModule, MatSelectModule, ReactiveFormsModule, RouterLink],
    templateUrl: './bulletin-board-list.component.html',
    styleUrls: ['./bulletin-board-list.component.scss']
})
export class BulletinBoardList implements AfterViewInit {
    filterTerm = '';
    filterColumn: 'message' | 'name' | 'id' | 'groupId' = 'message';
    filterForm: FormGroup;

    private bulletinsChanged$: Subject<void> = new Subject<void>();

    private _items: BulletinBoardItem[] = [];
    private destroyRef: DestroyRef = inject(DestroyRef);

    @ViewChild('scrollContainer') private scroll!: ElementRef;

    @Input() set bulletinBoardItems(items: BulletinBoardItem[]) {
        this._items = items;
        this.bulletinsChanged$.next();
    }

    get bulletinBoardItems(): BulletinBoardItem[] {
        return this._items;
    }

    @Output() filterChanged: EventEmitter<BulletinBoardFilterArgs> = new EventEmitter<BulletinBoardFilterArgs>();

    constructor(
        private formBuilder: FormBuilder,
        private nifiCommon: NiFiCommon
    ) {
        this.filterForm = this.formBuilder.group({ filterTerm: '', filterColumn: 'message' });
    }

    ngAfterViewInit(): void {
        this.filterForm
            .get('filterTerm')
            ?.valueChanges.pipe(debounceTime(500), takeUntilDestroyed(this.destroyRef))
            .subscribe((filterTerm: string) => {
                const filterColumn = this.filterForm.get('filterColumn')?.value;
                this.applyFilter(filterTerm, filterColumn);
            });

        this.filterForm
            .get('filterColumn')
            ?.valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
            .subscribe((filterColumn: string) => {
                const filterTerm = this.filterForm.get('filterTerm')?.value;
                this.applyFilter(filterTerm, filterColumn);
            });

        // scroll the initial chuck of bulletins
        this.scrollToBottom();

        this.bulletinsChanged$
            .pipe(delay(10)) // allow the new data a chance to render so the sizing of the scroll container is correct
            .subscribe(() => {
                // auto-scroll
                this.scrollToBottom();
            });
    }

    private scrollToBottom() {
        if (this.scroll) {
            this.scroll.nativeElement.scroll({
                top: this.scroll.nativeElement.scrollHeight,
                left: 0,
                behavior: 'smooth'
            });
        }
    }

    applyFilter(filterTerm: string, filterColumn: string) {
        this.filterChanged.next({
            filterColumn,
            filterTerm
        });
    }

    isBulletin(bulletinBoardItem: BulletinBoardItem): boolean {
        const item = bulletinBoardItem.item;
        return !('type' in item);
    }

    asBulletin(bulletinBoardItem: BulletinBoardItem): BulletinEntity | null {
        if (this.isBulletin(bulletinBoardItem)) {
            return bulletinBoardItem.item as BulletinEntity;
        }
        return null;
    }

    asBulletinEvent(bulletinBoardItem: BulletinBoardItem): BulletinBoardEvent | null {
        if (!this.isBulletin(bulletinBoardItem)) {
            return bulletinBoardItem.item as BulletinBoardEvent;
        }
        return null;
    }

    getSeverity(severity: string) {
        switch (severity.toLowerCase()) {
            case 'error':
                return 'bulletin-error mat-warn';
            case 'warn':
            case 'warning':
                return 'bulletin-warn';
            default:
                return 'bulletin-normal nifi-success-darker';
        }
    }

    getRouterLink(bulletin: BulletinEntity): string[] | null {
        const type: ComponentType | null = this.getComponentType(bulletin.bulletin.sourceType);
        if (type && bulletin.sourceId) {
            if (type === ComponentType.ControllerService) {
                if (bulletin.groupId) {
                    // process group controller service
                    return ['/process-groups', bulletin.groupId, 'controller-services', bulletin.sourceId];
                } else {
                    // management controller service
                    return ['/settings', 'management-controller-services', bulletin.sourceId];
                }
            }

            if (type === ComponentType.ReportingTask) {
                return ['/settings', 'reporting-tasks', bulletin.sourceId];
            }

            if (type === ComponentType.FlowRegistryClient) {
                return ['/settings', 'registry-clients', bulletin.sourceId];
            }

            if (type === ComponentType.FlowAnalysisRule) {
                return ['/settings', 'flow-analysis-rules', bulletin.sourceId];
            }

            if (type === ComponentType.ParameterProvider) {
                return ['/settings', 'parameter-providers', bulletin.sourceId];
            }

            if (bulletin.groupId) {
                return ['/process-groups', bulletin.groupId, type, bulletin.sourceId];
            }
        }
        return null;
    }

    private getComponentType(sourceType: string): ComponentType | null {
        switch (sourceType) {
            case 'PROCESSOR':
                return ComponentType.Processor;
            case 'REMOTE_PROCESS_GROUP':
                return ComponentType.RemoteProcessGroup;
            case 'INPUT_PORT':
                return ComponentType.InputPort;
            case 'OUTPUT_PORT':
                return ComponentType.OutputPort;
            case 'FUNNEL':
                return ComponentType.Funnel;
            case 'CONTROLLER_SERVICE':
                return ComponentType.ControllerService;
            case 'REPORTING_TASK':
                return ComponentType.ReportingTask;
            case 'FLOW_ANALYSIS_RULE':
                return ComponentType.FlowAnalysisRule;
            case 'PARAMETER_PROVIDER':
                return ComponentType.ParameterProvider;
            case 'FLOW_REGISTRY_CLIENT':
                return ComponentType.FlowRegistryClient;
            default:
                return null;
        }
    }
}
