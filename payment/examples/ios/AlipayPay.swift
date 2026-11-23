import Foundation
import AlipaySDK

// 支付宝 APP 支付(Swift + AlipaySDK)。
//
// 支付宝客户端支付不需要在本 App 里处理密码,用户是在支付宝 App / WebView 里完成,
// 本 App 只需要:
//  1. 把服务端下发并签好名的 order_string 交给支付宝 SDK;
//  2. 在回调里拿到结果,再查服务端状态。
enum AlipayPay {
    /// App 的 URL Scheme,需与支付宝开放平台配置一致(用于回跳)。
    static let scheme = "yourapp"

    /// 调起支付宝支付。
    ///
    /// - Parameters:
    ///   - params: session.client_params.alipay。
    ///   - completion: 回调结果,失败时 error 非空。
    ///
    /// 说明:不同版本的 AlipaySDK 方法签名略有差异( payOrder / payV2 ),
    /// 若编译报找不到方法,把 orderString 拼进 URL 交给系统打开也是常见做法:
    ///
    ///     let url = URL(string: orderString)!
    ///     UIApplication.shared.open(url)
    static func pay(with params: AlipayParams, completion: @escaping (Result<Void, Error>) -> Void) {
        let client = ALIPAYClient()
        client.payOrder(params.orderString, scheme: scheme, options: nil) { response in
            // response 为 AlipayOrderSuccessResponse / AlipayOrderFailureResponse 等。
            if let success = response as? AlipayOrderSuccessResponse {
                if success.resultCode == "9000" {
                    completion(.success(()))
                } else {
                    completion(.failure(NSError(
                        domain: "AlipayPay",
                        code: Int(success.resultCode ?? "0") ?? -1,
                        userInfo: [NSLocalizedDescriptionKey: success.errMsg ?? "支付宝支付未完成"])))
                }
            } else if let failure = response as? AlipayOrderFailureResponse {
                completion(.failure(NSError(
                    domain: "AlipayPay",
                    code: -1,
                    userInfo: [NSLocalizedDescriptionKey: failure.errMsg ?? failure.errInfo ?? "支付宝支付未完成"])))
            } else {
                completion(.failure(NSError(
                    domain: "AlipayPay",
                    code: -1,
                    userInfo: [NSLocalizedDescriptionKey: "支付宝支付未完成"])))
            }
        }
    }
}

// 用法:
//
//	func payAlipay() {
//	    Task {
//	        do {
//	            let resp = try await PaymentService().createOrder(
//	                channel: "alipay", tradeType: "app", amount: 19900)
//	            guard let params = resp.session.clientParams?.alipay else { return }
//	            AlipayPay.pay(with: params) { result in
//	                switch result {
//	                case .success:
//	                    // 授权完成,最终结果以服务端回调为准
//	                    break
//	                case .failure(let error):
//	                    print("支付宝支付失败: \(error.localizedDescription)")
//	                }
//	            }
//	        } catch {
//	            print("下单失败: \(error)")
//	        }
//	    }
//	}
