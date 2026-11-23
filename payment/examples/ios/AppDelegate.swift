import UIKit
import WeChatOpenSDK
import AlipaySDK

// AppDelegate:负责第三方 SDK 的注册与回调转发。
//
// 微信:需要在 application(_:open:options:) 里把 URL 交给 WXApi;
// 支付宝:回调在 application(_:open:options:) 里通过 notification 通知,
//       这里统一转成一次「支付完成」回调。
final class AppDelegate: UIResponder, UIApplicationDelegate {
    /// 支付完成回调(参数为错误信息,nil 表示用户已授权)。
    var paymentCompletion: ((Error?) -> Void)?

    func application(_ application: UIApplication,
                     didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        // 注册微信(在 AppDelegate 启动时注册一次即可)。
        WechatPay.register(appID: "wx1234567890abcdef", universalLink: "https://yourapp.example.com/")
        return true
    }

    func application(_ app: UIApplication,
                     open url: URL,
                     options: [UIApplication.OpenURLOptionsKey: Any] = [:]) -> Bool {
        // 微信回跳
        if WechatPay.handleOpenURL(url: url, completion: { err in
            self.paymentCompletion?(err)
        }) {
            return true
        }
        // 支付宝回跳:SDK 会发 Notification 通知,这里在通知里取结果。
        if url.scheme == AlipayPay.scheme {
            return true
        }
        return false
    }
}

// 支付宝结果通知处理(放在合适的位置注册,例如 NotificationCenter.default.addObserver)。
//
//	NotificationCenter.default.addObserver(
//	    forName: NSNotification.Name("AlipaySDKCallback"),
//	    object: nil,
//	    queue: .main
//	) { notification in
//	    let resp = notification.object as? AlipayOrderSuccessResponse
//	    let err: Error? = resp.map { r in
//	        r.resultCode == "9000" ? nil : NSError(
//	            domain: "AlipayPay", code: -1,
//	            userInfo: [NSLocalizedDescriptionKey: r.errMsg ?? "支付宝支付未完成"])
//	    }
//	    AppDelegate.shared?.paymentCompletion?(err)
//	}
//
// 微信与支付宝的「支付成功」都只是授权信号,最终以服务端回调入账为准;
// 客户端拿到信号后再查一次 /api/query 给用户即时反馈即可。
