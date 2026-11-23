import { defineConfig } from 'vitest/config';

// 前端测试配置。
//
// 用 jsdom 跑 DOM 相关测试(控制台页面全是手写 DOM,必须要有 DOM 环境)。
// include 只收 src/ 下的 *.test.ts —— 测试文件与源码放在一起,改哪个功能
// 就近加一个测试文件,不用另建 tests/ 目录去猜哪个测试对应哪个模块。
export default defineConfig({
  test: {
    environment: 'jsdom',
    include: ['src/**/*.test.ts'],
    setupFiles: ['src/test/setup.ts'],
    globals: false,
  },
});
