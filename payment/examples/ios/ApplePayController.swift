import Foundation
import PassKit

// Apple Pay(Swift + PassKit)。
//
// 与微信/支付宝不同:Apple Pay 不在 App 内输密码,而是弹出系统钱包面板,
// 用户用 Face ID / 密码 授权后,App 拿到一个「支付令牌」(paymentData),
// 必须把令牌回传服务端完成扣款。
//
// 两种常见形态:
//   - 直连商户:令牌就是 PKPayment.token.paymentData 的 base64,服务端直接用;
//   - 经收单网关(本库默认走这条,例如 Stripe):客户端用网关的 Apple Pay SDK
//     (如 StripeApplePay)完成授权,拿到 client_secret 对应的 paymentIntent,
//     再把令牌回传本库的服务端接口完成扣款。
//     服务端下发的 publishable_key / client_secret 就是给这一步用的。
final class ApplePayController: NSObject {
    /// 收银台展示按钮(放到 UI 里即可)。
    static let button = PKPaymentButton(paymentButtonType: .checkout, paymentButtonStyle: .black)

    private let params: ApplePayParams
    private let service: PaymentService
    private var controller: PKPaymentAuthorizationController?
    private var completion: ((Result<Void, Error>) -> Void)?

    init(params: ApplePayParams, service: PaymentService = PaymentService()) {
        self.params = params
        self.service = service
    }

    /// 拉起 Apple Pay。
    func present(completion: @escaping (Result<Void, Error>) -> Void) {
        self.completion = completion

        let request = PKPaymentRequest()
        request.merchantIdentifier = params.merchantID
        request.countryCode = params.countryCode.isEmpty ? "CN" : params.countryCode
        request.currencyCode = params.currency.isEmpty ? "CNY" : params.currency
        request.merchantCapabilities = [.capability3DS]
        request.supportedNetworks = [.visa, .masterCard, .amex, .discover]
        // 金额单位是分,Apple Pay 要的是「元」。
        request.paymentSummaryItems = [
            PKPaymentSummaryItem(label: params.displayName,
                                 amount: NSDecimalNumber(value: Double(params.amount) / 100.0))
        ]

        let controller = PKPaymentAuthorizationController(paymentRequest: request)
        controller.delegate = self
        self.controller = controller
        controller.present { presented in
            if !presented {
                completion(.failure(NSError(domain: "ApplePay", code: -1,
                                            userInfo: [NSLocalizedDescriptionKey: "无法拉起 Apple Pay,请检查设备支持与商户配置"])))
            }
        }
    }
}

extension ApplePayController: PKPaymentAuthorizationControllerDelegate {
    func paymentAuthorizationController(_ controller: PKPaymentAuthorizationController,
                                        didAuthorizePayment payment: PKPayment,
                                        handler completion: @escaping (PKPaymentAuthorizationResult) -> Void) {
        // 支付令牌:回传给服务端、由服务端去收单机构完成扣款的那个令牌。
        //
        // 这里有两种形态,取决于收单机构:
        //
        // 1) **直连收单机构**(本例默认):令牌就是 base64 编码的 paymentData,
        //    收单机构能直接解。
        // 2) **经收单网关(如 Stripe)**:服务端下发的是 client_secret,
        //    客户端要用网关自己的 SDK 完成授权,拿到 **PaymentMethod id**(pm_xxx)
        //    再回传;把原始 paymentData 回传过去网关是认不出来的。
        //    Stripe 的写法见下面注释。
        //
        // let token = payment.token.paymentData.base64EncodedString()
        let token = self.paymentToken(from: payment)

        Task {
            do {
                let result = try await service.chargeWallet(channel: "apple_pay",
                                                             outTradeNo: params.outTradeNo,
                                                             paymentToken: token,
                                                             amount: params.amount)
                if result.paid {
                    completion(PKPaymentAuthorizationResult(status: .success, errors: nil))
                    self.completion?(.success(()))
                } else {
                    completion(PKPaymentAuthorizationResult(status: .failure, errors: nil))
                    self.completion?(.failure(NSError(
                        domain: "ApplePay", code: -1,
                        userInfo: [NSLocalizedDescriptionKey: "扣款未完成,状态: \(result.status)"])))
                }
            } catch {
                completion(PKPaymentAuthorizationResult(status: .failure, errors: nil))
                self.completion?(.failure(error))
            }
        }
    }

    /// 从 PassKit 的授权结果里取出「可以回传给服务端」的支付令牌。
    ///
    /// 直连收单机构:base64 的 paymentData。
    /// Stripe 之类走网关的:要用网关 SDK 确认支付,拿到 PaymentMethod id。
    /// 这里给的是直连形态;走网关时把下面换成网关 SDK 的调用即可:
    ///
    ///     STPAPIClient.shared.confirmPayment(payment, clientContext: ctx) { status, paymentIntent, error in
    ///         // paymentIntent?.paymentMethod?.id  → "pm_xxx"
    ///     }
    private func paymentToken(from payment: PKPayment) -> String {
        payment.token.paymentData.base64EncodedString()
    }

    func paymentAuthorizationControllerDidFinish(_ controller: PKPaymentAuthorizationController) {
        controller.dismiss {
            self.controller = nil
        }
    }
}

// 用法:
//
//	func payApplePay() {
//	    Task {
//	        do {
//	            let resp = try await PaymentService().createOrder(
//	                channel: "apple_pay", tradeType: "wallet", amount: 19900)
//	            guard let params = resp.session.clientParams?.applePay else { return }
//	            ApplePayController(params: params).present { result in
//	                switch result {
//	                case .success: print("已授权,等待服务端回调入账")
//	                case .failure(let error): print("Apple Pay 失败: \(error.localizedDescription)")
//	                }
//	            }
//	        } catch {
//	            print("下单失败: \(error)")
//	        }
//	    }
//	}
