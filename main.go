package main

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// Album 表示专辑的结构体
type Album struct {
	Artist string `json:"artist"`
	Title  string `json:"title"`
	Year   string `json:"year"`
}

// PostResponse 表示上传专辑后的返回数据
type PostResponse struct {
	AlbumID   string `json:"albumID"`
	ImageSize string `json:"imageSize"`
}

// Review 表示一个 like 或 dislike 请求
type Review struct {
	AlbumID string `json:"albumID"`
	Action  string `json:"action"` // like 或 dislike
}

var db *sql.DB
var mqChannel *amqp.Channel

func main() {
	// 连接 MySQL 数据库
	dsn := "root:123456@tcp(127.0.0.1:3306)/mydemodb"
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("DB open failed: %v", err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatalf("DB ping failed: %v", err)
	}

	// 创建 albums 表（如果不存在）
	db.Exec(`CREATE TABLE IF NOT EXISTS albums (
		albumID VARCHAR(36) NOT NULL UNIQUE,
		image LONGBLOB,
		profile JSON
	)`)

	// 初始化 RabbitMQ 连接和队列
	setupRabbitMQ()

	// 初始化 HTTP 路由
	r := gin.Default()
	r.GET("/health", healthCheck)                       // 健康检查
	r.POST("/albums", doPost)                           // 上传专辑
	r.GET("/albums/:albumID", goGet)                    // 获取专辑信息
	r.POST("/review/:likeornot/:albumID", handleReview) // 提交 like 或 dislike

	// 启动 HTTP 服务
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Server running on port %s", port)
	r.Run(":" + port)
}

// 初始化 RabbitMQ 连接与队列
func setupRabbitMQ() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("RabbitMQ connect error: %v", err)
	}
	mqChannel, err = conn.Channel()
	if err != nil {
		log.Fatalf("RabbitMQ channel error: %v", err)
	}
	_, err = mqChannel.QueueDeclare("reviewQueue", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Queue declare error: %v", err)
	}
}

// 健康检查接口
func healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// 上传专辑接口，接收图片和 profile 数据
func doPost(c *gin.Context) {
	file, _, err := c.Request.FormFile("image")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Image is required"})
		return
	}
	defer file.Close()
	imageBytes, _ := io.ReadAll(file)

	profile := c.PostForm("profile")
	albumID := uuid.New().String()

	_, err = db.Exec("INSERT INTO albums (albumID, image, profile) VALUES (?, ?, ?)", albumID, imageBytes, profile)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "DB insert error"})
		return
	}
	c.JSON(http.StatusCreated, PostResponse{AlbumID: albumID, ImageSize: string(len(imageBytes))})
}

// 获取专辑 profile 信息（不包含图片）
func goGet(c *gin.Context) {
	albumID := c.Param("albumID")
	var profile string
	err := db.QueryRow("SELECT profile FROM albums WHERE albumID = ?", albumID).Scan(&profile)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "Not found"})
		return
	} else if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "DB query error"})
		return
	}
	c.Data(http.StatusOK, "application/json", []byte(profile))
}

// 提交 like 或 dislike 的接口，异步发送消息到 RabbitMQ
func handleReview(c *gin.Context) {
	likeOrNot := c.Param("likeornot")
	albumID := c.Param("albumID")

	if likeOrNot != "like" && likeOrNot != "dislike" {
		c.JSON(http.StatusBadRequest, gin.H{"msg": "Invalid review action"})
		return
	}

	// Check if album exists
	var exists bool
	err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM albums WHERE albumID = ?)", albumID).Scan(&exists)
	if err != nil || !exists {
		c.JSON(http.StatusNotFound, gin.H{"msg": "Album not found"})
		return
	}

	// 创建新的 Channel：每次请求新建一个 Channel
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"msg": "Failed to connect to RabbitMQ"})
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"msg": "Failed to open channel"})
		return
	}
	defer ch.Close()

	// 构建 Review 消息并发送
	review := Review{AlbumID: albumID, Action: likeOrNot}
	data, _ := json.Marshal(review)

	err = ch.Publish("", "reviewQueue", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"msg": "Failed to publish to queue"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"msg": "Review accepted"})
}
