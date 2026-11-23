/**
 * 请求序号守卫。
 *
 * 页面上常有多个请求并发(用户连改两次筛选条件、作用域连切两次)。
 * 后到的响应**可能来自更早的那个请求** —— 于是:
 *
 *   用户把状态从「全部」改成「支付成功」再改成「已关闭」,
 *   三个请求同时在飞;第一个(全部)最后回来,
 *   把表格覆盖成「全部订单」,而下拉框停在「已关闭」。
 *
 * 界面自相矛盾,商户据此判断就会看错数据 —— 这比报错更危险。
 *
 * 用法:
 *   const guard = new SeqGuard();
 *   const seq = guard.next();
 *   const res = await api(...);
 *   if (!guard.isCurrent(seq)) return;   // 已经有更新的请求了,丢弃这次结果
 */
export class SeqGuard {
  private seq = 0;

  /** 领一个序号:发请求前调用。 */
  next(): number {
    this.seq += 1;
    return this.seq;
  }

  /** 这个序号还是最新的吗?不是的话,这次响应已经过时了。 */
  isCurrent(seq: number): boolean {
    return seq === this.seq;
  }

  /** 让所有在途请求都作废(离开页面时调用)。 */
  invalidate(): void {
    this.seq += 1;
  }
}
