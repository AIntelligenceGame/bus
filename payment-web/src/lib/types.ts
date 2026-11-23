/** 后端接口的数据结构。 */

export interface User {
  user_id: string;
  user_name: string;
  password_set?: boolean;
  mfa_enabled: boolean;
  mfa_type: string;
  mfa_target: string;
  /** 商户自己配置的对外访问地址(回调地址前缀);空 = 用平台默认。 */
  notify_base_url?: string;
  /** 是否强制订单绑定应用:开启后每一笔支付都必须归属某个应用。 */
  require_app_binding?: boolean;
}

export interface WechatConfig {
  app_id?: string;
  mch_id?: string;
  serial_no?: string;
  private_key_pem?: string;
  api_v3_key?: string;
  cert_dir?: string;
  auto_fetch_certs?: boolean;
}

export interface AlipayConfig {
  app_id?: string;
  private_key_pem?: string;
  alipay_public_key_pem?: string;
  alipay_public_cert_path?: string;
  app_cert_path?: string;
  root_cert_path?: string;
  gateway?: string;
}

export interface WalletConfig {
  merchant_id?: string;
  display_name?: string;
  domain_name?: string;
  currency?: string;
  stripe_secret_key?: string;
  stripe_publishable_key?: string;
  stripe_webhook_secret?: string;
}

export interface ChannelConfig {
  channel: string;
  enabled: boolean;
  is_default: boolean;
  wechat?: WechatConfig;
  alipay?: AlipayConfig;
  wallet?: WalletConfig;
}

export interface MeResponse {
  user: User;
  channels: ChannelConfig[];
  default_channel: string;
  notify_urls: Record<string, string>;
  /** 商户自己配置的对外访问地址(空 = 用平台默认)。 */
  notify_base_url?: string;
  /** 平台默认的对外访问地址(启动参数 -base)。 */
  notify_base_default?: string;
}

export interface LoginResponse {
  user_id: string;
  name: string;
  token: string;
  created: boolean;
  mfa_enabled: boolean;
  mfa_type: string;
  mfa_target: string;
  /** 二次验证绑定在、但验证码发不出去,本次登录跳过了它。 */
  mfa_skipped?: boolean;
  /** 后端对「跳过二次验证」的说明。 */
  mfa_note?: string;
}

export interface SecurityResponse {
  user_id: string;
  name: string;
  password_set: boolean;
  mfa_enabled: boolean;
  mfa_type: string;
  mfa_target: string;
  verify_ttl_sec: number;
  /** 邮件验证码下发通道是否已配置(没配的话邮件码只写日志)。 */
  email_channel_configured?: boolean;
  /** 短信验证码下发通道是否已配置。 */
  sms_channel_configured?: boolean;
  /**
   * 当前二次验证绑定**能不能真的把验证码发出去**。
   *
   * false = 绑定还在、但验证码只会写进日志,用户永远收不到 ——
   * 这种二次验证拦不住任何人,平台登录时会跳过它,并建议关掉或改用验证器 App。
   */
  mfa_deliverable?: boolean;
  /** 是否绑了备用验证方式(验证器 App 丢了时的第二条腿)。 */
  mfa_backup?: boolean;
  /** 备用验证方式(email/sms)。 */
  mfa_backup_type?: string;
  /** 备用接收地址(已脱敏)。 */
  mfa_backup_target?: string;
  /**
   * 备用方式的验证码**能不能真的发出去**。
   *
   * false = 绑了,但通道发不出去(没配 SMTP/短信网关,或平台以 -log-mfa 启动):
   * 码只写进服务器日志,用户永远收不到 —— 这条退路等于没有。
   */
  mfa_backup_deliverable?: boolean;
}

/** 验证码下发通道配置(敏感字段只回显「有没有」,不回显内容)。 */
export interface MFAChannel {
  configured?: boolean;
  host?: string;
  port?: number;
  username?: string;
  from_addr?: string;
  /** 邮件通道加密方式:starttls / tls / none。 */
  tls_mode?: string;
  url?: string;
  /** 短信网关取验证码的字段名。 */
  code_field?: string;
  /** 短信网关取接收地址的字段名。 */
  target_field?: string;
  /** 授权码是否已保存。 */
  password_set?: boolean;
  /** 网关令牌是否已保存。 */
  token_set?: boolean;
}

export interface VerifyResponse {
  verify_token: string;
  expires_in: number;
}

/** 商户的一个应用 / 产品。 */
export interface App {
  app_id: string;
  user_id?: string;
  name: string;
  remark?: string;
  /** 该应用下的订单数。 */
  order_count?: number;
  /** 该应用下的成功金额合计(分)。 */
  amount?: number;
  created_at?: string;
  updated_at?: string;
}

/** 「未分类」订单的汇总(app_id 为空的那批)。 */
export interface UnclassifiedSummary {
  order_count: number;
  success_count: number;
  /** 成功金额合计(分)。 */
  amount: number;
}

/** 应用列表接口的响应。 */
export interface AppsResponse {
  apps: App[];
  unclassified: UnclassifiedSummary;
}

/**
 * 合并后的渠道配置(应用专属覆盖全局)。
 *
 * app_specific=true 表示这一条是**该应用单独配的**;
 * false 表示继承全局配置。
 */
export interface AppChannelConfig extends ChannelConfig {
  app_specific?: boolean;
}

/** 某个应用的渠道配置响应。 */
export interface AppChannelsResponse {
  app: App;
  channels: AppChannelConfig[];
  notify_urls: Record<string, string>;
}

/** 分组统计里的一组。 */
export interface StatGroup {
  /** 分组键:应用 ID / 渠道 / 日期(YYYY-MM-DD)。 */
  key: string;
  /** 分组显示名(应用名 / 渠道名 / 日期)。 */
  label: string;
  order_count: number;
  success_count: number;
  refund_count: number;
  /** 成功金额合计(分)。 */
  total_amount: number;
  /** 退款金额合计(分)。 */
  refund_amount: number;
  /** 净收入(分)。 */
  net_amount: number;
}

/** 带分组的统计响应。 */
export interface StatsGroupedResponse {
  stats: Stats;
  group_by: string;
  groups: StatGroup[];
  /**
   * 结果是否被截断(按天分组时扫描行数超上限,更早的数据没统计进来)。
   * 为 true 时页面要显式提示,不能让人以为看到的就是全部。
   */
  truncated?: boolean;
}

export interface OrderRecord {
  out_trade_no: string;
  /** 订单归属的应用(空 = 未分类)。 */
  app_id?: string;
  channel: string;
  trade_type: string;
  subject: string;
  amount: number;
  amount_yuan?: string;
  /** 已退金额(分)。 */
  refunded_amount?: number;
  /** 还能退的金额(分)= amount - refunded_amount。 */
  refundable_amount?: number;
  status: string;
  qrcode?: string;
  transaction_id?: string;
  created_at?: string;
  updated_at?: string;
}

export interface AuditLog {
  /** 后端返回的是 log_id(字符串),不是数字 id。 */
  log_id: string;
  action: string;
  target: string;
  detail: string;
  client_ip: string;
  created_at: string;
}

/** 退款流水。 */
export interface RefundRecord {
  refund_id: string;
  user_id: string;
  out_trade_no: string;
  out_refund_no: string;
  channel: string;
  /** 退款归属的应用(继承自订单)。 */
  app_id?: string;
  amount: number;
  amount_yuan: string;
  status: string;
  reason?: string;
  created_at: string;
}

/** 开放接口密钥。 */
export interface APIKeyRecord {
  key_id: string;
  user_id?: string;
  name: string;
  /** 绑定的应用(空 = 不绑定,走全局渠道配置)。 */
  app_id?: string;
  scopes: string[];
  enabled: boolean;
  expires_at?: string;
  last_used_at?: string;
  created_at?: string;
}

/** 交易统计。 */
export interface Stats {
  order_count: number;
  success_count: number;
  failed_count: number;
  refund_count: number;
  total_amount: number;
  refund_amount: number;
  net_amount: number;
  today_count: number;
  today_amount: number;
  success_rate: number;
}

/** 分页响应(订单 / 审计日志通用)。 */
export interface PageResponse {
  page: number;
  size: number;
  total: number;
  pages: number;
}

/** 微信 APP 支付参数(字段名与各端 SDK 对齐)。 */
export interface WechatClientParams {
  app_id: string;
  partner_id: string;
  prepay_id: string;
  nonce_str: string;
  time_stamp: string;
  package_value: string;
  sign: string;
  out_trade_no: string;
  amount: number;
}

/** 支付宝 APP 支付参数。 */
export interface AlipayClientParams {
  app_id: string;
  order_string: string;
  out_trade_no: string;
  amount: number;
}

/** Apple Pay 参数(仅 iOS)。 */
export interface ApplePayClientParams {
  merchant_id: string;
  display_name: string;
  domain_name: string;
  country_code: string;
  currency: string;
  amount: number;
  out_trade_no: string;
  gateway: string;
  publishable_key: string;
  client_secret: string;
}

/** Google Pay 参数。 */
export interface GooglePayClientParams {
  gateway: string;
  publishable_key: string;
  merchant_name: string;
  merchant_id: string;
  currency: string;
  amount: number;
  out_trade_no: string;
  environment: string;
  client_secret: string;
}

/** 面向客户端 SDK 的结构化调起参数,只有当前渠道对应的字段有值。 */
export interface ClientParams {
  wechat?: WechatClientParams;
  alipay?: AlipayClientParams;
  apple_pay?: ApplePayClientParams;
  google_pay?: GooglePayClientParams;
}

export interface PaySession {
  channel?: string;
  trade_type?: string;
  out_trade_no?: string;
  /** 微信预支付会话标识(APP / JSAPI 拉起用)。 */
  prepay_id?: string;
  redirect_url?: string;
  qrcode?: string;
  token?: string;
  /**
   * 移动端拉起支付用的扁平参数集合。
   *
   * 值不一定是字符串(渠道可能塞数字/布尔),所以类型放宽成 unknown。
   */
  app_params?: Record<string, unknown>;
  client_params?: ClientParams;
}

export interface PayResponse {
  session: PaySession;
  order_no: string;
  amount: number;
  yuan: string;
  notify_url: string;
  qrcode_url: string;
  usage: string[];
}
