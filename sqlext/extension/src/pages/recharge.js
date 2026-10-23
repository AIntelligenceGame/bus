// recharge.js
import { send, msg, busy, fmtTime } from './ui.js';

const $ = (s) => document.querySelector(s);

const CHANNEL_LABEL = {
  manual: '人工转账（推荐，无需商户资质）',
  wechat: '微信支付',
  alipay: '支付宝',
  stripe: 'Stripe（信用卡）'
};

async function loadBalance() {
  const res = await send('balance');
  if (res && res.ok) {
    $('#balance').textContent = res.result.balance + ' 积分';
    $('#cost').textContent = res.result.cost_per_parse + ' 积分/次';
  } else {
    msg($('#msg'), (res && res.error) || '读取余额失败', 'err');
  }
}

async function loadChannels() {
  const sel = $('#channel');
  // 渠道列表由服务端下发，避免前端写死
  try {
    const res = await fetch(await apiBase() + '/api/v1/billing/channels');
    const data = await res.json();
    const names = (data && data.channels) || ['manual'];
    sel.innerHTML = '';
    for (const n of names) {
      const o = document.createElement('option');
      o.value = n;
      o.textContent = CHANNEL_LABEL[n] || n;
      sel.appendChild(o);
    }
  } catch (_) {
    sel.innerHTML = '<option value="manual">人工转账</option>';
  }
}

async function apiBase() {
  const res = await send('getApiBase');
  return (res && res.ok && res.result.apiBase) || '';
}

async function loadOrders() {
  const res = await send('listOrders');
  const ul = $('#orders');
  ul.innerHTML = '';
  if (!res || !res.ok) {
    ul.innerHTML = '<li class="hint">读取订单失败</li>';
    return;
  }
  const orders = res.result.orders || [];
  if (!orders.length) {
    ul.innerHTML = '<li class="hint">还没有订单</li>';
    return;
  }
  for (const o of orders) {
    const li = document.createElement('li');
    const st = { pending: '待支付', paid: '已到账', canceled: '已取消' }[o.status] || o.status;
    li.innerHTML = `<span>${o.id} · ${o.amount} 积分<br><span class="hint">${fmtTime(o.created_at)}</span></span>
                    <span>${st}</span>`;
    ul.appendChild(li);
  }
}

$('#btn').onclick = async (e) => {
  const amount = Number($('#amount').value);
  const channel = $('#channel').value;
  busy(e.target, true);
  msg($('#msg'), '');
  const res = await send('createOrder', { amount, channel });
  busy(e.target, false);
  if (!res || !res.ok) {
    msg($('#msg'), (res && res.error) || '创建订单失败', 'err');
    return;
  }
  const { order, checkout } = res.result;
  const box = $('#pay-box');
  box.style.display = '';
  const body = $('#pay-body');
  body.innerHTML = '';

  if (checkout.method === 'qrcode' && checkout.qrcode_url) {
    const img = document.createElement('img');
    img.className = 'qr';
    img.src = checkout.qrcode_url;
    body.appendChild(img);
  }
  const p = document.createElement('p');
  p.className = 'hint';
  p.textContent = checkout.instructions || '请按提示完成支付。';
  body.appendChild(p);
  const code = document.createElement('p');
  code.className = 'hint';
  code.innerHTML = '订单号：<code>' + order.id + '</code>';
  body.appendChild(code);

  msg($('#msg'), '订单已创建', 'ok');
  loadOrders();
  loadBalance();
};

loadBalance();
loadChannels();
loadOrders();
