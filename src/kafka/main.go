package main

import (
	"fmt"
	"github.com/Shopify/sarama"
)

func main()  {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	client, err := sarama.NewSyncProducer([]string{"120.78.213.214:9092"}, config)
	if err != nil {
		fmt.Println("new producer error, err:", err)
		return
	}
	defer client.Close()

	msg := &sarama.ProducerMessage{}
	msg.Topic = "test"
	msg.Value = sarama.StringEncoder("this is a test message")

	pid, offset, err := client.SendMessage(msg)
	if err != nil {
		fmt.Printf("send message error:", err)
		return
	}

	fmt.Printf("pid:%v, offset:%v\n", pid, offset)
}
