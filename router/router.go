package router

import (
	_ "KafkaScale/docs"
	"github.com/gin-gonic/gin"
	"KafkaScale/router/middleware"
	"KafkaScale/handler/sd"
	"net/http"
	"KafkaScale/handler"

	"github.com/swaggo/gin-swagger"
	"github.com/swaggo/gin-swagger/swaggerFiles"
)

func Load(g *gin.Engine, mw ...gin.HandlerFunc) *gin.Engine {
	// Middlewares.
	g.Use(gin.Recovery())
	g.Use(middleware.NoCache)
	g.Use(middleware.Options)
	g.Use(middleware.Secure)
	g.Use(mw...)
	// 404 Handler.
	g.NoRoute(func(c *gin.Context) {
		c.String(http.StatusNotFound, "The incorrect API route.")
	})

	// The health check handlers
	svcd := g.Group("/sd")
	{
		svcd.GET("/health", sd.HealthCheck)
		svcd.GET("/disk", sd.DiskCheck)
		svcd.GET("/cpu", sd.CPUCheck)
		svcd.GET("/ram", sd.RAMCheck)
	}

	u := g.Group("/")
	{
		u.GET("/metric/:task_name", handler.GetMetric)
		u.GET("/metrics", handler.GetAllMetric)
		u.POST("/task", handler.PostTask)
		u.POST("/task/local", handler.PostTaskLocal)
		u.GET("/kill", handler.Kill)
	}

	// swagger api docs
	g.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	return g
}