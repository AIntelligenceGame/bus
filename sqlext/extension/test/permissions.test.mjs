// permissions.test.mjs —— 守住一条容易踩的 Chrome 规则：
//
//   chrome.permissions.request() 必须在**扩展页面**里、由用户手势触发。
//   在 Service Worker 里调用会静默失败（不报错，但也不申请权限）。
//
// 症状很隐蔽：授权流程"看起来成功了"，但那个网站上根本没有 window.sqlext，
// 因为内容脚本注册因为缺主机权限而失败。
//
// 所以这里做静态检查：permissions.request 只能出现在 src/pages/ 下。

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, relative } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const SRC = join(here, '..', 'src');

/** 递归列出目录下的所有 .js 文件。 */
function jsFiles(dir) {
  const out = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...jsFiles(p));
    } else if (entry.name.endsWith('.js')) {
      out.push(p);
    }
  }
  return out;
}

/**
 * 去掉注释，只留真正的代码。
 * 不去掉的话，注释里提一句 "chrome.permissions.request" 就会被误判成调用。
 */
function stripComments(src) {
  return src
    .replace(/\/\*[\s\S]*?\*\//g, '')  // 块注释
    .replace(/(^|[^:])\/\/.*$/gm, '$1'); // 行注释（避开 http:// 这种 URL）
}

test('chrome.permissions.request 只能出现在扩展页面里，不能在 service worker 里', () => {
  const offenders = [];
  for (const file of jsFiles(SRC)) {
    const rel = relative(SRC, file);
    const code = stripComments(readFileSync(file, 'utf8'));
    if (!code.includes('chrome.permissions.request')) {
      continue;
    }
    // src/pages/ 下的页面有用户手势上下文，可以用
    if (rel.startsWith('pages' + '/') || rel.startsWith('pages\\')) {
      continue;
    }
    offenders.push(rel);
  }
  assert.deepEqual(
    offenders,
    [],
    '这些文件里调用了 chrome.permissions.request，但它们不在 src/pages/ 下：\n' +
      offenders.join('\n') +
      '\n（Service Worker 里没有用户手势，调用会静默失败）'
  );
});

test('service worker 确实不再自己申请权限', () => {
  const sw = stripComments(readFileSync(join(SRC, 'background', 'service-worker.js'), 'utf8'));
  assert.ok(
    !sw.includes('chrome.permissions.request'),
    'service-worker.js 不该直接调用 chrome.permissions.request'
  );
});
