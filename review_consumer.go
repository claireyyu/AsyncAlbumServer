package main

import (
	"database/sql"
	"encoding/json"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
)

// Review 表示从队列中接收到的 like 或 dislike 消息结构
type Review struct {
	AlbumID string `json:"albumID"`
	Action  string `json:"action"` // like 或 dislike
}

func main() {
	// === 建立 RabbitMQ 连接 ===
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("RabbitMQ connect error: %v", err)
	}
	defer conn.Close()

	// === 创建 RabbitMQ 通道 ===
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("RabbitMQ channel error: %v", err)
	}
	defer ch.Close()

	// === 声明队列，确保队列存在（与生产者队列名一致）===
	_, err = ch.QueueDeclare("reviewQueue", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Queue declare error: %v", err)
	}

	// === 注册消费者，订阅队列中的消息 ===
	msgs, err := ch.Consume("reviewQueue", "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Consumer error: %v", err)
	}

	// === 连接 MySQL 数据库 ===
	db, err := sql.Open("mysql", "root:12345678@tcp(database-1.cqzfidh4zvkc.us-west-2.rds.amazonaws.com:3306)/mydemodb")
	if err != nil {
		log.Fatalf("DB connect error: %v", err)
	}
	defer db.Close()

	// === 创建 reviews 表（如果不存在）===
	db.Exec(`CREATE TABLE IF NOT EXISTS reviews (
		id INT AUTO_INCREMENT PRIMARY KEY,
		albumID VARCHAR(36),
		action VARCHAR(10)
	)`)

	// === 启动消息处理循环，持续监听消息 ===
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var review Review

			// 将消息体反序列化为 Review 对象
			err := json.Unmarshal(d.Body, &review)
			if err != nil {
				log.Printf("Message decode error: %v", err)
				d.Nack(false, false)
				continue
			}

			// 将评论信息写入数据库
			_, err = db.Exec("INSERT INTO reviews (albumID, action) VALUES (?, ?)", review.AlbumID, review.Action)
			if err != nil {
				log.Printf("DB insert error: %v", err)
				d.Nack(false, true) // 消息重试
				continue
			}

			// 消息处理成功后确认 ack
			d.Ack(false)
			log.Printf("Review saved: albumID=%s, action=%s", review.AlbumID, review.Action)
		}
	}()

	log.Println("Consumer running. Waiting for messages...")
	<-forever
}
