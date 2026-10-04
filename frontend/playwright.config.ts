import {defineConfig,devices} from '@playwright/test';
export default defineConfig({testDir:'./tests',use:{baseURL:process.env.PLAYWRIGHT_BASE_URL??'http://localhost:5173',trace:'retain-on-failure'},projects:[{name:'chromium',use:{...devices['Desktop Chrome']}}],reporter:'list'});
