package main

import (

	"github.com/childe/gohangout/codec"
	"github.com/golang/glog"
	"github.com/apache/pulsar-client-go/pulsar"
	"context"
)

type PulsarInput struct{
	config         map[interface{}]interface{}
	decoder codec.Decoder
	messages chan *pulsar.Message
	consumer pulsar.Consumer
	client   pulsar.Client

}

func New(config map[interface{}]interface{}) interface{} {

	var (
		codertype      string = "plain"
		 serviceUrl, topic, subscriptionName string
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
	//glog.Fatalf("%s", config["serviceUrl"])
	/*
	serviceUrl := config["serviceUrl"].(string)
	topic := config["topic"].(string)
	subscriptionName := config["subscriptionName"].(string)
	*/
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: serviceUrl})
	// defer client.Close()
	c, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
		Type:             pulsar.Shared,
	})
	if err != nil {
		glog.Fatalf("could not init Consumer: %s", err)
	}
	// defer c.Close()
	go func() {
		for {
			msg, err := c.Receive(context.Background())
			if err == nil {
				PulsarInput.messages <- &msg
			} else {
				// The client will automatically try to recover from all errors.
				glog.Errorf("Consumer error: %v (%v)\n", err, msg)
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
	p.consumer.Close()
	p.client.Close()
}
