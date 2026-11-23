/**
 * 金额换算(单位:分 ↔ 元)。
 *
 * 单独成文件、不依赖别的东西,是为了让任何模块都能用而不会绕成循环依赖
 * (ui.ts ↔ verify.ts ↔ api.ts 之间已经有环了)。
 */

/** 金额(分)转元字符串。 */
export function money(cents: number): string {
  const v = Math.round(Number.isFinite(cents) ? cents : 0);
  const abs = Math.abs(v);
  // 整数运算,不用 (v/100).toFixed(2):
  // 浮点除法在某些值上会得到 19.899999999999999 这种,
  // toFixed 之后看着对,但拿去和别的金额比较就翻车。
  const yuan = Math.floor(abs / 100);
  const fen = String(abs % 100).padStart(2, '0');
  return (v < 0 ? '-' : '') + yuan + '.' + fen;
}

/**
 * 「元」字符串 → 分(精确解析,不用浮点)。
 *
 * 为什么不能写 `Math.round(Number(x) * 100)`:
 *
 *   - 8.115 * 100 在浮点里是 811.4999999999999,四舍五入成 811(8.11 元)。
 *     用户明明输入的是 8.115,钱却少了 5 厘 —— 而且是**静默**少掉的。
 *   - 输入 "1e3" 会被当成 1000 元(科学计数法),多半不是用户的意思。
 *   - 输入 "12.345" 会被悄悄截成 12.34;后端 YuanToCents 是**直接报错**的,
 *     前端悄悄改数、后端以为收到的是合法值,两边对不上账。
 *
 * 返回 null 表示输入不合法(空 / 非数字 / 超过两位小数 / 超出安全整数)。
 * 调用方要据此给用户明确提示,而不是自己兜个底继续往下走。
 */
export function yuanToCents(text: string): number | null {
  const s = (text || '').trim();
  if (!s) return null;
  // 只接受「数字」或「数字.最多两位小数」,不接受负号、科学计数法、空格。
  const m = /^(\d+)(?:\.(\d{1,2}))?$/.exec(s);
  if (!m) return null;
  const yuan = Number(m[1]);
  const fen = Number((m[2] || '').padEnd(2, '0'));
  if (!Number.isSafeInteger(yuan)) return null;
  const cents = yuan * 100 + fen;
  if (!Number.isSafeInteger(cents) || cents <= 0) return null;
  return cents;
}
