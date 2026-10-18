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

import { Component, computed, inject, input, output } from '@angular/core';
import { RouterLink, RouterLinkActive } from '@angular/router';
import { Storage } from '@nifi/shared';
import { selectFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.selectors';
import { Store } from '@ngrx/store';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { selectClusterSummary } from '../../../../state/cluster-summary/cluster-summary.selectors';

@Component({
    selector: 'modern-navigation',
    imports: [RouterLink, RouterLinkActive],
    templateUrl: './modern-navigation.component.html',
    styleUrl: './modern-navigation.component.scss'
})
export class ModernNavigationComponent {
    private store = inject(Store);
    user = this.store.selectSignal(selectCurrentUser);
    cluster = this.store.selectSignal(selectClusterSummary);
    private storage = inject(Storage);
    designer = input(false);
    configuration = this.store.selectSignal(selectFlowConfiguration);
    canvasLink(): string {
        const route = this.storage.getItem<string>('current-canvas-route');
        return typeof route === 'string' && route.startsWith('/process-groups/') ? route : '/process-groups/root';
    }
    links = computed(() => {
        const user = this.user();
        const links = [
            { route: '/summary', label: 'Summary', title: 'Summary', icon: 'fa fa-table' },
            { route: '/bulletins', label: 'Bulletins', title: 'Bulletin Board', icon: 'fa fa-bell-o' },
            { route: '/parameter-contexts', label: 'Parameters', title: 'Parameter Contexts', icon: 'fa fa-list-alt' },
            {
                route: '/flow-configuration-history',
                label: 'Flow history',
                title: 'Flow Configuration History',
                icon: 'fa fa-history'
            }
        ];
        if (user?.provenancePermissions.canRead)
            links.push({
                route: '/provenance',
                label: 'Provenance',
                title: 'Data Provenance',
                icon: 'icon icon-provenance'
            });
        if (user?.controllerPermissions.canRead)
            links.push({ route: '/settings', label: 'Settings', title: 'Controller Settings', icon: 'fa fa-sliders' });
        if (user?.connectorsPermissions.canRead)
            links.push({ route: '/connectors', label: 'Connectors', title: 'Connectors', icon: 'fa fa-plug' });
        if (user?.countersPermissions.canRead)
            links.push({ route: '/counters', label: 'Counters', title: 'Counter', icon: 'icon icon-counter' });
        if (this.cluster()?.clustered && user?.controllerPermissions.canRead)
            links.push({ route: '/cluster', label: 'Cluster', title: 'Cluster', icon: 'fa fa-cubes' });
        if (this.configuration()?.supportsManagedAuthorizer) {
            if (user?.tenantsPermissions.canRead)
                links.push({ route: '/users', label: 'Users', title: 'Users', icon: 'fa fa-users' });
            if (user?.policiesPermissions.canRead && user.policiesPermissions.canWrite)
                links.push({ route: '/access-policies', label: 'Policies', title: 'Policies', icon: 'fa fa-key' });
        }
        return links;
    });
    overviewRequested = output<void>();
    canvasRequested = output<void>();
}
