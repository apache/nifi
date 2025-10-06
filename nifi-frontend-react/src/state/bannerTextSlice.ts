import { createSlice, PayloadAction } from '@reduxjs/toolkit';

export interface BannerText {
  headerText?: string;
  footerText?: string;
  [key: string]: unknown;
}

export interface BannerTextState {
  status: 'idle' | 'loading' | 'success' | 'error';
  bannerText?: BannerText;
  error?: string;
}

const initialState: BannerTextState = {
  status: 'idle'
};

const bannerTextSlice = createSlice({
  name: 'bannerText',
  initialState,
  reducers: {
    setBannerTextStatus(state, action: PayloadAction<BannerTextState['status']>) {
      state.status = action.payload;
      if (action.payload !== 'error') {
        state.error = undefined;
      }
    },
    setBannerText(state, action: PayloadAction<BannerText | undefined>) {
      state.bannerText = action.payload;
    },
    setBannerTextError(state, action: PayloadAction<string | undefined>) {
      state.error = action.payload ?? 'Unknown error';
      state.status = 'error';
    },
    resetBannerText() {
      return initialState;
    }
  }
});

export const { setBannerTextStatus, setBannerText, setBannerTextError, resetBannerText } =
  bannerTextSlice.actions;

export default bannerTextSlice.reducer;
