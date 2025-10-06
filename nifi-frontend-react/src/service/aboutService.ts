// AboutService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { Observable } from 'rxjs';

import type { About } from '../state/aboutSlice';

export const API = '../nifi-api';

export interface AboutResponse {
  about: About;
  [key: string]: unknown;
}

export function getAbout$(): Observable<AboutResponse> {
  return ajax.getJSON<AboutResponse>(`${API}/flow/about`);
}
