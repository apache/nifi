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

import { RouterModule, Routes } from '@angular/router';
import { NgModule } from '@angular/core';
import { Users } from './users.component';
import { authorizationGuard } from '../../../service/guard/authorization.guard';
import { CurrentUser } from '../../../state/current-user';

const routes: Routes = [
    {
        path: '',
        component: Users,
        canMatch: [authorizationGuard((user: CurrentUser) => user.tenantsPermissions.canRead)],
        children: [
            {
                path: ':id',
                component: Users,
                children: [
                    {
                        path: 'edit',
                        component: Users
                    },
                    {
                        path: 'policies',
                        component: Users
                    }
                ]
            }
        ]
    }
];

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class UsersRoutingModule {}
