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
import { MatButtonModule } from '@angular/material/button';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { MatTableDataSource, MatTableModule } from '@angular/material/table';
import { MatSortModule, Sort } from '@angular/material/sort';

import { AccessPolicySummaryEntity, ComponentReferenceEntity } from '../../../../../state/shared';
import { RouterLink } from '@angular/router';
import { UserAccessPoliciesDialogRequest } from '../../../state/user-listing';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';
import { ComponentType, SelectOption, CloseOnEscapeDialog, NiFiCommon } from '@nifi/shared';

@Component({
    selector: 'user-access-policies',
    templateUrl: './user-access-policies.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        MatSortModule,
        RouterLink,
        MatMenu,
        MatMenuItem,
        MatMenuTrigger
    ],
    styleUrls: ['./user-access-policies.component.scss']
})
export class UserAccessPolicies extends CloseOnEscapeDialog {
    displayedColumns: string[] = ['policy', 'action', 'actions'];
    dataSource: MatTableDataSource<AccessPolicySummaryEntity> = new MatTableDataSource<AccessPolicySummaryEntity>();
    selectedPolicyId: string | null = null;

    sort: Sort = {
        active: 'policy',
        direction: 'asc'
    };

    constructor(
        @Inject(MAT_DIALOG_DATA) public request: UserAccessPoliciesDialogRequest,
        private nifiCommon: NiFiCommon
    ) {
        super();
        this.dataSource.data = this.sortPolicies(request.accessPolicies, this.sort);
    }

    updateSort(sort: Sort): void {
        this.sort = sort;
        this.dataSource.data = this.sortPolicies(this.dataSource.data, sort);
    }

    sortPolicies(policies: AccessPolicySummaryEntity[], sort: Sort): AccessPolicySummaryEntity[] {
        const data: AccessPolicySummaryEntity[] = policies.slice();
        return data.sort((a, b) => {
            const isAsc = sort.direction === 'asc';

            let retVal = 0;
            if (a.permissions.canRead && b.permissions.canRead) {
                switch (sort.active) {
                    case 'policy':
                        retVal = this.nifiCommon.compareString(this.formatPolicy(a), this.formatPolicy(b));
                        break;
                    case 'action':
                        retVal = this.nifiCommon.compareString(a.component.action, b.component.action);
                        break;
                }
            } else {
                if (!a.permissions.canRead && !b.permissions.canRead) {
                    retVal = 0;
                }
                if (a.permissions.canRead) {
                    retVal = 1;
                } else {
                    retVal = -1;
                }
            }

            return retVal * (isAsc ? 1 : -1);
        });
    }

    formatPolicy(policy: AccessPolicySummaryEntity): string {
        if (policy.component.resource.startsWith('/restricted-components')) {
            // restricted components policy
            return this.restrictedComponentResourceParser(policy);
        } else if (policy.component.componentReference) {
            // not restricted/global policy... check if user has access to the component reference
            return this.componentResourceParser(policy);
        } else {
            // may be a global policy
            const policyValue: string = this.nifiCommon.substringAfterLast(policy.component.resource, '/');
            const policyOption: SelectOption | undefined = this.nifiCommon.getPolicyTypeListing(policyValue);

            // if known global policy, format it otherwise format as unknown
            if (policyOption) {
                return this.globalResourceParser(policyOption);
            } else {
                return this.unknownResourceParser(policy);
            }
        }
    }

    /**
     * Generates a human-readable restricted component policy string.
     *
     * @returns {string}
     * @param policy
     */
    private restrictedComponentResourceParser(policy: AccessPolicySummaryEntity): string {
        const resource: string = policy.component.resource;

        if (resource === '/restricted-components') {
            return 'Restricted components regardless of restrictions';
        }

        const subResource = this.nifiCommon.substringAfterFirst(resource, '/restricted-components/');
        return `Restricted components requiring '${subResource}'`;
    }

    /**
     * Generates a human-readable component policy string.
     *
     * @returns {string}
     * @param policy
     */
    private componentResourceParser(policy: AccessPolicySummaryEntity): string {
        let resource: string = policy.component.resource;
        let policyLabel = '';

        // determine policy type
        if (resource.startsWith('/policies')) {
            resource = this.nifiCommon.substringAfterFirst(resource, '/policies');
            policyLabel += 'Admin policy for ';
        } else if (resource.startsWith('/data-transfer')) {
            resource = this.nifiCommon.substringAfterFirst(resource, '/data-transfer');
            policyLabel += 'Site to site policy for ';
        } else if (resource.startsWith('/data')) {
            resource = this.nifiCommon.substringAfterFirst(resource, '/data');
            policyLabel += 'Data policy for ';
        } else if (resource.startsWith('/operation')) {
            resource = this.nifiCommon.substringAfterFirst(resource, '/operation');
            policyLabel += 'Operate policy for ';
        } else {
            policyLabel += 'Component policy for ';
        }

        if (resource.startsWith('/processors')) {
            policyLabel += 'processor ';
        } else if (resource.startsWith('/controller-services')) {
            policyLabel += 'controller service ';
        } else if (resource.startsWith('/funnels')) {
            policyLabel += 'funnel ';
        } else if (resource.startsWith('/input-ports')) {
            policyLabel += 'input port ';
        } else if (resource.startsWith('/labels')) {
            policyLabel += 'label ';
        } else if (resource.startsWith('/output-ports')) {
            policyLabel += 'output port ';
        } else if (resource.startsWith('/process-groups')) {
            policyLabel += 'process group ';
        } else if (resource.startsWith('/remote-process-groups')) {
            policyLabel += 'remote process group ';
        } else if (resource.startsWith('/reporting-tasks')) {
            policyLabel += 'reporting task ';
        } else if (resource.startsWith('/parameter-contexts')) {
            policyLabel += 'parameter context ';
        }

        const componentReference: ComponentReferenceEntity | undefined = policy.component.componentReference;
        if (componentReference) {
            if (componentReference.permissions.canRead) {
                policyLabel += componentReference.component.name;
            } else {
                policyLabel += componentReference.id;
            }
        }

        return policyLabel;
    }

    /**
     * Generates a human-readable global policy string.
     *
     * @param policy
     * @returns {string}
     */
    globalResourceParser(policy: SelectOption): string {
        return `Global policy to ${policy.text}`;
    }

    /**
     * Generates a human-readable policy string for an unknown resource.
     *
     * @returns {string}
     * @param policy
     */
    unknownResourceParser(policy: AccessPolicySummaryEntity): string {
        return `Unknown resource ${policy.component.resource}`;
    }

    canGoToPolicyTarget(policy: AccessPolicySummaryEntity): boolean {
        return policy.permissions.canRead && policy.component.componentReference != null;
    }

    getPolicyTargetLink(policy: AccessPolicySummaryEntity): string[] {
        const resource: string = policy.component.resource;

        // @ts-ignore
        const componentReference: ComponentReferenceEntity = policy.component.componentReference;

        if (resource.indexOf('/processors') >= 0) {
            return [
                '/process-groups',
                // @ts-ignore
                componentReference.parentGroupId,
                ComponentType.Processor,
                componentReference.id
            ];
        } else if (resource.indexOf('/controller-services') >= 0) {
            if (componentReference.parentGroupId) {
                return [
                    '/process-groups',
                    componentReference.parentGroupId,
                    'controller-services',
                    componentReference.id
                ];
            } else {
                return ['/settings', 'management-controller-services', componentReference.id];
            }
        } else if (resource.indexOf('/funnels') >= 0) {
            // @ts-ignore
            return ['/process-groups', componentReference.parentGroupId, ComponentType.Funnel, componentReference.id];
        } else if (resource.indexOf('/input-ports') >= 0) {
            return [
                '/process-groups',
                // @ts-ignore
                componentReference.parentGroupId,
                ComponentType.InputPort,
                componentReference.id
            ];
        } else if (resource.indexOf('/labels') >= 0) {
            // @ts-ignore
            return ['/process-groups', componentReference.parentGroupId, ComponentType.Label, componentReference.id];
        } else if (resource.indexOf('/output-ports') >= 0) {
            return [
                '/process-groups',
                // @ts-ignore
                componentReference.parentGroupId,
                ComponentType.OutputPort,
                componentReference.id
            ];
        } else if (resource.indexOf('/process-groups') >= 0) {
            if (componentReference.parentGroupId) {
                return [
                    '/process-groups',
                    componentReference.parentGroupId,
                    ComponentType.ProcessGroup,
                    componentReference.id
                ];
            } else {
                return ['/process-groups', componentReference.id];
            }
        } else if (resource.indexOf('/remote-process-groups') >= 0) {
            return [
                '/process-groups',
                // @ts-ignore
                componentReference.parentGroupId,
                ComponentType.RemoteProcessGroup,
                componentReference.id
            ];
        } else if (resource.indexOf('/reporting-tasks') >= 0) {
            return ['/settings', 'reporting-tasks', componentReference.id];
        } else if (resource.indexOf('/parameter-contexts') >= 0) {
            return ['/parameter-contexts', componentReference.id];
        }
        return ['/'];
    }

    selectPolicy(policy: AccessPolicySummaryEntity): void {
        this.selectedPolicyId = policy.id;
    }

    isSelected(policy: AccessPolicySummaryEntity): boolean {
        if (this.selectedPolicyId) {
            return policy.id == this.selectedPolicyId;
        }
        return false;
    }
}
