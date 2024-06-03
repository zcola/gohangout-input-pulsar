package main

import (

	"github.com/childe/gohangout/codec"
	"github.com/golang/glog"
	"github.com/apache/pulsar-client-go/pulsar"
	"context"
	"time"
)

type PulsarInput struct{
	config         map[interface{}]interface{}
	decoder codec.Decoder
	messages chan *pulsar.Message
	consumer pulsar.Consumer
	client   pulsar.Client
	isShuttingDown bool

}

func New(config map[interface{}]interface{}) interface{} {

	var (
		codertype      string = "plain"
		serviceUrl, topic, subscriptionName string
		enableTransaction bool = true
	)
	if codecV, ok := config["codec"]; ok {
		codertype = codecV.(string)
	}
	if v, ok := config["serviceUrl"]; !ok {
		glog.Fatal("Pulsar input must have serviceUrl")
	} else {
		serviceUrl = v.(string)
	}
	if v, ok := config["topic"]; !ok {
		glog.Fatal("Pulsar input must have topic")
	} else {
		topic = v.(string)
	}
	if v, ok := config["subscriptionName"]; !ok {
		glog.Fatal("Pulsar input must have subscriptionName")
	} else {
		subscriptionName = v.(string)
	}
	PulsarInput := &PulsarInput{
		config:         config,
		messages:       make(chan *pulsar.Message, 10),
		decoder:        codec.NewDecoder(codertype),
	}
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: serviceUrl, EnableTransaction: enableTransaction })
	if err != nil {
		glog.Fatal("init pulsar error, exception is %s", err)
	}
	PulsarInput.client = client
	//defer client.Close()
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		glog.Fatalf("could not init Consumer: %s", err)
	}
	PulsarInput.consumer = consumer
	go func() {
		var txn pulsar.Transaction // 声明事务变量
		//var err error
		var msgCount int // 消息计数器
		const batchSize = 1000 // 设置批处理大小为1000
		for {
			if PulsarInput.isShuttingDown {
				return
			}
			msg, err := consumer.Receive(context.Background())
			if err != nil {
				glog.Errorf("Consumer error: %v (%v)\n", err, msg)
				continue
			}

			// 当计数器为0时，表示一个新的批次开始，创建一个新的事务
			if msgCount%batchSize == 0 {
				if txn != nil {
					// 确保前一个事务已正确提交
					if err := txn.Commit(context.Background()); err != nil {
						glog.Errorf("failed to commit transaction: %v", err)
					}
				}
				// 创建新事务
				txn, err = client.NewTransaction(1 * time.Minute)
				if err != nil {
					glog.Errorf("failed to create transaction: %v", err)
					continue
				}
			}

			// 处理消息
			PulsarInput.messages <- &msg
			msgCount++

			// 当达到1000条消息时，提交事务
			if msgCount%batchSize == 0 {
				if err := txn.Commit(context.Background()); err != nil {
					glog.Errorf("failed to commit transaction: %v", err)
				} else {
					// 仅在事务提交成功后确认消息
					consumer.Ack(msg)
				}
				txn = nil // 重置事务变量，以便下一个循环创建新事务
			}
		}

		// 循环结束后，确保最后一个事务被提交
		if txn != nil && msgCount%batchSize != 0 {
			if err := txn.Commit(context.Background()); err != nil {
				glog.Errorf("failed to commit transaction: %v", err)
			}
		}
	}()
	return PulsarInput
}

//ReadOneEvent 单次事件的处理函数
func (p *PulsarInput) ReadOneEvent() map[string]interface{} {
	message := <-p.messages
	event := p.decoder.Decode((*message).Payload())
	return event
}

//Shutdown 关闭需要做的事情
func (p *PulsarInput) Shutdown() {
	p.isShuttingDown = true
	// close(p.messages)
	p.consumer.Close()
	p.client.Close()
}
