import {defineConfig} from 'vitest/config';
import react from '@vitejs/plugin-react';
export default defineConfig({plugins:[react()],server:{watch:{usePolling:true,interval:750},proxy:{'/api':'http://localhost:8080'}},test:{include:['src/**/*.test.ts','src/**/*.test.tsx'],maxWorkers:1,environment:'jsdom',setupFiles:['./src/test-setup.ts']}});
