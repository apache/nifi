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

import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { FlowDesigner } from './flow-designer.component';
import { RootGroupRedirector } from '../ui/root/redirector/root-group-redirector.component';
import { rootGroupGuard } from '../ui/root/guard/root-group.guard';

const routes: Routes = [
    {
        path: 'process-groups/:processGroupId',
        component: FlowDesigner,
        children: [
            {
                path: 'controller-services',
                loadChildren: () =>
                    import('../ui/controller-service/controller-services.module').then(
                        (m) => m.ControllerServicesModule
                    )
            },
            {
                path: '',
                loadChildren: () => import('../ui/canvas/canvas.module').then((m) => m.CanvasModule)
            }
        ]
    },
    {
        path: 'remote-process-group/:rpgId',
        component: FlowDesigner,
        children: [
            {
                path: 'manage-remote-ports',
                loadChildren: () =>
                    import('../ui/manage-remote-ports/manage-remote-ports.module').then(
                        (m) => m.ManageRemotePortsModule
                    )
            }
        ]
    },
    { path: '', component: RootGroupRedirector, canActivate: [rootGroupGuard], pathMatch: 'full' }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class FlowDesignerRoutingModule {}
