package handler

import (
	"net/http"

	"github.com/AIntelligenceGame/bus/config"
	"github.com/gin-gonic/gin"
)

func AddPool(ctx *gin.Context) {
	config.Work.Add(1)
	ctx.JSON(http.StatusOK, gin.H{
		"msg":    "Success",
		"status": 200,
	})
}
func DelPool(ctx *gin.Context) {
	config.Work.Done()
	ctx.JSON(http.StatusOK, gin.H{
		"msg":    "Success",
		"status": 200,
	})
}
