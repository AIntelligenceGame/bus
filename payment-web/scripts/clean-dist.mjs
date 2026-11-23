/**
 * 清理上一次的前端打包产物。
 *
 * 为什么需要:Vite 的产物文件名带 hash(assets/app-<hash>.js),
 * 每构建一次就是**新名字**,旧文件不会被覆盖、也不会自动删除。
 * 而 Go 端是 `//go:embed all:web/dist` —— 目录里剩多少个文件就打包多少个文件,
 * 于是二进制里会同时塞进三四个历史版本的前端,体积白白翻几倍。
 *
 * 所以每次打包前先把旧产物清掉:
 *   - 删掉 assets/ 整个目录;
 *   - 删掉 index.html;
 *   - **保留 dist/.gitkeep** —— Go 端 //go:embed 要求目录存在,
 *     目录整个没了编译直接失败。
 *
 * 由 package.json 的 prebuild 钩子自动调用。
 */
import { rmSync, existsSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const dist = join(here, '..', '..', 'payment', 'platform', 'web', 'dist');

rmSync(join(dist, 'assets'), { recursive: true, force: true });
rmSync(join(dist, 'index.html'), { force: true });

// .gitkeep 不能删,但目录必须在。
if (!existsSync(dist)) mkdirSync(dist, { recursive: true });

console.log('已清理旧打包产物:' + dist);
