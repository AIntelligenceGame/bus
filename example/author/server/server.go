package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pquerna/otp/totp"
)

func main() {
	r := gin.Default()

	// 添加 CORS 中间件
	r.Use(CORS())

	// 生成密钥
	secret, err := totp.Generate(totp.GenerateOpts{
		Issuer:      "123",
		AccountName: "user@example.com",
	})
	if err != nil {
		fmt.Println("Error generating secret:", err)
		return
	}

	// 生成二维码 URL
	qrCodeUrl := secret.URL()
	fmt.Println("QR Code URL:", qrCodeUrl)

	// 接口 1: 获取二维码
	r.GET("/qr", func(c *gin.Context) {
		c.Redirect(http.StatusFound, qrCodeUrl)
	})

	// 接口 2: 验证动态码
	r.POST("/verify", func(c *gin.Context) {
		var request struct {
			Code string `json:"code"`
		}
		fmt.Println("Received code:", request.Code)
		if err := c.BindJSON(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format"})
			return
		}
		fmt.Println("Received code pass:", request.Code)

		// 假设 secret 是从数据库中获取的
		secret := secret.Secret()

		// 验证动态码
		valid := totp.Validate(request.Code, secret)
		if valid {
			fmt.Println("Validate code :", secret)
			c.JSON(http.StatusOK, gin.H{"success": true})
		} else {
			fmt.Println("Validate code fail:", secret)
			c.JSON(http.StatusUnauthorized, gin.H{"success": false})
		}
	})

	// 接口 3: 重定向到 helloworld 页面
	r.GET("/helloworld", func(c *gin.Context) {
		c.HTML(http.StatusOK, "helloworld.html", nil)
	})

	// 启动服务
	fmt.Println("Starting server at port 8080")
	if err := r.Run(":8080"); err != nil {
		fmt.Println("Server error:", err)
	}
}

// CORS 中间件
func CORS() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.JSON(200, nil)
			return
		}
		c.Next()
	}
}
