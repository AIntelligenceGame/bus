import Foundation

// 支付服务端接口封装。
//
// 对应 examples/server 提供的三个接口:
//
//	POST /api/pay            下单
//	POST /api/wallet/charge  钱包扣款
//	GET  /api/query          查单
final class PaymentService {
    /// 服务端地址。
    ///
    /// 默认值对应 `go run ./payment/examples/server`(监听 8088)。
    /// 真机调试要换成电脑在局域网里的地址,比如 http://192.168.1.10:8088。
    static let baseURL = URL(string: "http://127.0.0.1:8088")!

    /// 业务侧登录票据,生产环境换成你们自己的 token。
    var authToken: String = ""

    private let session: URLSession

    init(session: URLSession = .shared) {
        self.session = session
    }

    // MARK: - 下单

    /// 下单,返回含 client_params 的支付会话。
    ///
    /// - Parameters:
    ///   - channel: 渠道(wechat / alipay / apple_pay / google_pay)。
    ///   - tradeType: 交易类型(app / wallet / native),nil 表示服务端自动推断。
    ///   - amount: 金额,单位:分。
    func createOrder(channel: String,
                     tradeType: String? = nil,
                     outTradeNo: String? = nil,
                     subject: String? = nil,
                     amount: Int64,
                     payerID: String? = nil) async throws -> PayResponse {
        let body = PayRequest(channel: channel,
                              tradeType: tradeType,
                              outTradeNo: outTradeNo,
                              subject: subject,
                              amount: amount,
                              payerID: payerID)
        let data = try JSONEncoder().encode(body)
        let (respData, response) = try await send(path: "/api/pay", method: "POST", body: data)
        try checkStatus(response, data: respData)
        return try JSONDecoder().decode(PayResponse.self, from: respData)
    }

    // MARK: - 钱包扣款

    /// 用客户端 SDK 回传的支付令牌完成扣款(Apple Pay / Google Pay)。
    func chargeWallet(channel: String,
                      outTradeNo: String,
                      paymentToken: String,
                      amount: Int64) async throws -> ChargeResponse {
        let body = ChargeRequest(channel: channel,
                                  outTradeNo: outTradeNo,
                                  paymentToken: paymentToken,
                                  amount: amount)
        let data = try JSONEncoder().encode(body)
        let (respData, response) = try await send(path: "/api/wallet/charge", method: "POST", body: data)
        try checkStatus(response, data: respData)
        return try JSONDecoder().decode(ChargeResponse.self, from: respData)
    }

    // MARK: - 查单

    /// 查单。客户端显示的「支付成功」只是授权信号,最终状态以服务端回调为准,
    /// 这里查到的结果用于给用户一个即时的状态反馈。
    func query(channel: String, outTradeNo: String) async throws -> QueryResponse {
        var components = URLComponents(url: Self.baseURL.appendingPathComponent("/api/query"),
                                       resolvingAgainstBaseURL: false)!
        components.queryItems = [
            URLQueryItem(name: "channel", value: channel),
            URLQueryItem(name: "out_trade_no", value: outTradeNo)
        ]
        var request = URLRequest(url: components.url!)
        request.httpMethod = "GET"
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        if !authToken.isEmpty {
            request.setValue(authToken, forHTTPHeaderField: "Authorization")
        }
        let (data, response) = try await session.data(for: request)
        try checkStatus(response, data: data)
        return try JSONDecoder().decode(ChargeResponse.self, from: data)
    }

    // MARK: - 内部

    private func send(path: String, method: String, body: Data?) async throws -> (Data, URLResponse) {
        var request = URLRequest(url: Self.baseURL.appendingPathComponent(path))
        request.httpMethod = method
        request.httpBody = body
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        if !authToken.isEmpty {
            request.setValue(authToken, forHTTPHeaderField: "Authorization")
        }
        return try await session.data(for: request)
    }

    private func checkStatus(_ response: URLResponse, data: Data) throws {
        guard let http = response as? HTTPURLResponse else { return }
        guard (200..<300).contains(http.statusCode) else {
            let text = String(data: data, encoding: .utf8) ?? ""
            throw NSError(domain: "PaymentService", code: http.statusCode,
                          userInfo: [NSLocalizedDescriptionKey: "接口返回 \(http.statusCode): \(text)"])
        }
    }
}
