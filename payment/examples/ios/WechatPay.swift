import Foundation
import WeChatOpenSDK

// 微信 APP 支付(Swift + WeChatOpenSDK)。
//
// 完整链路:
//
//	1. 服务端下单拿到 client_params.wechat;
//	2. 组装 WXApi.PayReq,交给 WXApi 调起微信;
//	3. 用户在微信里输密码;
//	4. 回调通知里拿到结果,再用订单号查服务端状态。
//
// 注意:微信 APP 支付的结果最终以服务端回调为准,客户端的 resp 只用于给用户即时反馈。
enum WechatPay {
    /// 注册微信开放平台 AppID(在 AppDelegate 启动时调用一次)。
    static func register(appID: String, universalLink: String) {
        WXApi.registerApp(appID, universalLink: universalLink)
    }

    /// 是否已安装微信(用于提示用户先装微信)。
    static func isWXApiInstalled() -> Bool {
        WXApi.isWXApiInstalled()
    }

    /// 用服务端下发的参数调起微信支付。
    ///
    /// - Parameters:
    ///   - params: session.client_params.wechat。
    ///   - completion: 微信返回结果(nil 表示用户取消或调用成功,具体看 error)。
    static func pay(with params: WechatParams, completion: @escaping (Error?) -> Void) {
        let req = WXApi.PayReq()
        req.partnerId = params.partnerID
        req.prepayId = params.prepayID
        req.nonceStr = params.nonceStr
        // timeStamp 是字符串,SDK 要 UInt32。
        req.timeStamp = UInt32(params.timeStamp) ?? 0
        req.packageValue = params.packageValue
        req.sign = params.sign

        WXApi.requestOrder(req, completion: completion)
    }

    /// 解析微信回调(在 AppDelegate 的 onReq 里调用)。
    ///
    /// 返回 true 表示已处理,AppDelegate 应返回 true 交给微信 SDK。
    static func handleOpenURL(url: URL, completion: @escaping (Error?) -> Void) -> Bool {
        WXApi.handleOpen(url, completion: completion)
    }
}

// 用法(在 ViewModel 或 ViewController 里):
//
//	func payWechat() {
//	    Task {
//	        do {
//	            let resp = try await PaymentService().createOrder(
//	                channel: "wechat", tradeType: "app", amount: 19900)
//	            guard let params = resp.session.clientParams?.wechat else { return }
//	            WechatPay.pay(with: params) { err in
//	                if let err = err {
//	                    print("微信支付未完成: \(err)")
//	                } else {
//	                    // 用户已授权,最终结果以服务端回调为准
//	                    Task { _ = try? await PaymentService().query(channel: "wechat", outTradeNo: resp.orderNo) }
//	                }
//	            }
//	        } catch {
//	            print("下单失败: \(error)")
//	        }
//	    }
//	}
