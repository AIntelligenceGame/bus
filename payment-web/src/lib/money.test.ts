import { describe, expect, it } from 'vitest';
import { money, yuanToCents } from './money';

describe('money(分 → 元)', () => {
  it('普通金额', () => {
    expect(money(0)).toBe('0.00');
    expect(money(1)).toBe('0.01');
    expect(money(99)).toBe('0.99');
    expect(money(100)).toBe('1.00');
    expect(money(19900)).toBe('199.00');
    expect(money(123456)).toBe('1234.56');
  });

  it('负数(退款)', () => {
    expect(money(-1)).toBe('-0.01');
    expect(money(-100)).toBe('-1.00');
    expect(money(-19900)).toBe('-199.00');
  });

  it('非数字按 0 处理,不能出现 NaN', () => {
    expect(money(NaN)).toBe('0.00');
    expect(money(Infinity)).toBe('0.00');
    expect(money(-Infinity)).toBe('0.00');
    expect(money(undefined as unknown as number)).toBe('0.00');
  });

  it('四舍五入到分,不出现浮点尾巴', () => {
    // 这些值用 (v/100).toFixed(2) 在某些平台上会得到 19.899999999999999。
    expect(money(1989)).toBe('19.89');
    expect(money(1999)).toBe('19.99');
    expect(money(1)).toBe('0.01');
  });
});

describe('yuanToCents(元 → 分)', () => {
  it('正常金额', () => {
    expect(yuanToCents('1')).toBe(100);
    expect(yuanToCents('1.0')).toBe(100);
    expect(yuanToCents('1.00')).toBe(100);
    expect(yuanToCents('1.5')).toBe(150);
    expect(yuanToCents('0.01')).toBe(1);
    expect(yuanToCents('199.00')).toBe(19900);
  });

  it('去掉首尾空格', () => {
    expect(yuanToCents('  1.5  ')).toBe(150);
  });

  it('空输入不合法', () => {
    expect(yuanToCents('')).toBeNull();
    expect(yuanToCents('   ')).toBeNull();
  });

  it('超过两位小数必须拒绝,不能悄悄截断', () => {
    // 后端 YuanToCents 是直接报错的,前端悄悄截成 12.34 会两边对不上账。
    expect(yuanToCents('12.345')).toBeNull();
    expect(yuanToCents('0.001')).toBeNull();
  });

  it('负数 / 科学计数法 / 非法字符一律拒绝', () => {
    expect(yuanToCents('-1')).toBeNull();
    expect(yuanToCents('-1.00')).toBeNull();
    expect(yuanToCents('1e3')).toBeNull();
    expect(yuanToCents('abc')).toBeNull();
    expect(yuanToCents('1.2.3')).toBeNull();
    expect(yuanToCents('Infinity')).toBeNull();
    expect(yuanToCents('NaN')).toBeNull();
  });

  it('零和负数金额不合法(后端也要求金额 > 0)', () => {
    expect(yuanToCents('0')).toBeNull();
    expect(yuanToCents('0.00')).toBeNull();
  });

  it('超出安全整数范围要拒绝', () => {
    expect(yuanToCents('99999999999999999999')).toBeNull();
  });

  it('往返一致:元 → 分 → 元', () => {
    // 用期望值显式列出来,而不是靠字符串补零 —— 补零的写法本身就是
    // 一处容易写错的地方,测试自己别犯同样的错。
    const cases: [string, number, string][] = [
      ['0.01', 1, '0.01'],
      ['1', 100, '1.00'],
      ['1.5', 150, '1.50'],
      ['199.00', 19900, '199.00'],
      ['1234.56', 123456, '1234.56'],
      ['0.99', 99, '0.99'],
    ];
    for (const [yuan, wantCents, wantYuan] of cases) {
      const cents = yuanToCents(yuan);
      expect(cents, yuan).toBe(wantCents);
      expect(money(cents as number), yuan).toBe(wantYuan);
    }
  });
});
