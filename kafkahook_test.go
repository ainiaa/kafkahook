package kafkahook

import (
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

func TestNewSyncHook(t *testing.T) {
	log := logrus.New()

	config := sarama.NewConfig()
	//等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	//随机向partition发送消息
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	//是否等待成功和失败后的响应,只有上面的RequireAcks设置不是NoReponse这里才有用.
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	//设置使用的kafka版本,如果低于V0_10_0_0版本,消息中的timestrap没有作用.需要消费和生产同时配置
	//注意，版本设置不对的话，kafka会返回很奇怪的错误，并且无法成功发送消息
	config.Version = sarama.V0_10_0_1
	var client sarama.Client
	var err error
	client,err = sarama.NewClient([]string{"10.23.30.3:9092"}, config)
	if err != nil {
		fmt.Printf("sarama.NewClient error:%s\n", err.Error())
	}
	var producer sarama.SyncProducer
	producer,err = sarama.NewSyncProducerFromClient(client)
	if err != nil {
		fmt.Printf("sarama.NewSyncProducerFromClient error:%s\n", err.Error())
	}

	hook := NewSyncHook("go_kafka_log", producer, WithFormatter(&logrus.TextFormatter{}))
	log.Hooks.Add(hook)

	log.Info("test log info")

	log.Warn("test log warn")

	log.Error("test log error")

	log.Fatal("test log fatal")
}

func TestNewAsyncHook(t *testing.T) {
	log := logrus.New()

	config := sarama.NewConfig()
	//等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	//设置使用的kafka版本,如果低于V0_10_0_0版本,消息中的timestrap没有作用.需要消费和生产同时配置
	//注意，版本设置不对的话，kafka会返回很奇怪的错误，并且无法成功发送消息
	config.Version = sarama.V0_10_0_1
	var client sarama.Client
	var err error
	client,err = sarama.NewClient([]string{"10.23.30.3:9092"}, config)
	if err != nil {
		fmt.Printf("sarama.NewClient error:%s\n", err.Error())
	}
	var producer sarama.AsyncProducer
	producer,err = sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		fmt.Printf("sarama.NewAsyncProducerFromClient error:%s\n", err.Error())
	}

	hook := NewAsyncHook("go_kafka_log", producer, WithFormatter(&logrus.TextFormatter{}))
	log.Hooks.Add(hook)

	log.Info("test log info")

	log.Warn("test log warn")

	log.Error("test log error")

	log.Fatal("test log fatal")
}