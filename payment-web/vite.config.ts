import { defineConfig } from 'vite';

// 构建产物直接落到 ../payment/platform/web/dist,由 Go 端 //go:embed 打进二进制。
// 前端源码保留在本目录(bus/payment-web),消费 payment 包的人拿到的只是打包产物。
// base 用相对路径,控制台挂在任意前缀(/、/payment/ ...)下都能工作。
export default defineConfig({
  base: './',
  build: {
    outDir: '../payment/platform/web/dist',
    // 不用 emptyOutDir:dist 里的 .gitkeep 是 go:embed 的占位文件,不能被删掉。
    // 旧产物由 npm prebuild 钩子(scripts/clean-dist.mjs)清理 ——
    // 产物文件名带 hash,不清理的话旧文件会一直堆着,被 //go:embed 一起打进二进制。
    emptyOutDir: false,
    assetsDir: 'assets',
    target: 'es2019',
    cssCodeSplit: false,
    sourcemap: false,
    reportCompressedSize: false,
    rollupOptions: {
      output: {
        // 关闭自动分包,单文件 bundle 便于 embed 与缓存
        manualChunks: undefined,
        entryFileNames: 'assets/app-[hash].js',
        chunkFileNames: 'assets/[name]-[hash].js',
        assetFileNames: 'assets/[name]-[hash][extname]',
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': { target: 'http://127.0.0.1:8080', changeOrigin: true },
      '/notify': { target: 'http://127.0.0.1:8080', changeOrigin: true },
    },
  },
});
