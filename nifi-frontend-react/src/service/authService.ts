// AuthService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { map } from 'rxjs/operators';
import { Observable } from 'rxjs';

import type { LoginConfiguration } from '../state/loginConfigurationSlice';

export const API = '../nifi-api';

export interface LoginConfigResponse {
  authenticationConfiguration: LoginConfiguration;
  [key: string]: unknown;
}

export function getLoginConfiguration$(): Observable<LoginConfigResponse> {
  return ajax
    .getJSON<LoginConfigResponse>(`${API}/authentication/configuration`)
    .pipe(map((response) => response));
}

export function login$(username: string, password: string): Observable<string> {
  const params = new URLSearchParams({ username, password });
  return ajax({
    url: `${API}/access/token`,
    method: 'POST',
    body: params,
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    responseType: 'text',
  }).pipe(map(response => response.response as string));
}

export function logout$(): Observable<any> {
  return ajax({
    url: `${API}/access/logout`,
    method: 'POST',
  }).pipe(map(response => response.response));
}
