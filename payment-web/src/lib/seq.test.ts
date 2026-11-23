import { describe, expect, it } from 'vitest';
import { SeqGuard } from './seq';

describe('SeqGuard', () => {
  it('刚领的序号就是当前的', () => {
    const guard = new SeqGuard();
    const seq = guard.next();
    expect(guard.isCurrent(seq)).toBe(true);
  });

  it('领了新序号之后,旧序号不再算数', () => {
    const guard = new SeqGuard();
    const old = guard.next();
    guard.next();
    expect(guard.isCurrent(old)).toBe(false);
  });

  it('invalidate 让所有在途请求都作废', () => {
    const guard = new SeqGuard();
    const seq = guard.next();
    guard.invalidate();
    expect(guard.isCurrent(seq)).toBe(false);
  });

  it('序号单调递增', () => {
    const guard = new SeqGuard();
    expect(guard.next()).toBeLessThan(guard.next());
  });
});
