// BannerTextService migrated from Angular with RxJS
import { ajax } from 'rxjs/ajax';
import { Observable } from 'rxjs';

import type { BannerText } from '../state/bannerTextSlice';

export const API = '../nifi-api';

export interface BannerTextResponse {
  banners: BannerText;
  [key: string]: unknown;
}

export function getBannerText$(): Observable<BannerTextResponse> {
  return ajax.getJSON<BannerTextResponse>(`${API}/flow/banners`);
}
