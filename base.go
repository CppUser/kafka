package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"log"
	"sync"
	"time"
)

type Message struct {
	RequestID string `json:"request_id"`
	Service   string `json:"service"`
	Action    string `json:"action"`
	Payload   string `json:"payload"`
}

type Client struct {
	Producer      sarama.SyncProducer
	Consumer      sarama.Consumer
	waitGroup     sync.WaitGroup
	responseChans map[string]chan *Message // Used for waiting for responses per RequestID
	mux           sync.Mutex
}

func NewClient(brokers []string) (*Client, error) {
	log.Printf("Creating new kafka client")
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true
	config.Version = sarama.V3_6_0_0

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &Client{
		Producer:      producer,
		Consumer:      consumer,
		responseChans: make(map[string]chan *Message),
	}, nil
}

func (kc *Client) Close() error {
	if err := kc.Producer.Close(); err != nil {
		return fmt.Errorf("failed to close producer: %w", err)
	}
	if err := kc.Consumer.Close(); err != nil {
		return fmt.Errorf("failed to close consumer: %w", err)
	}
	return nil
}

/////////////////////////REFACTOR MOVE TO ITS OWN FILES////////////////////////////////////

func (p *Client) SendMessage(serviceName, action, payload string) error {
	message := Message{
		RequestID: uuid.New().String(),
		Service:   serviceName,
		Action:    action,
		Payload:   payload,
	}

	msgBt, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	topic := fmt.Sprintf("%s_requests", serviceName)

	kmsg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msgBt),
	}

	partition, offset, err := p.Producer.SendMessage(kmsg)
	if err != nil {
		return err
	}
	log.Printf("Message sent to partition %d at offset %d\n", partition, offset)
	return nil
}

func (kc *Client) SendMessageWithResponse(service, action, payload string) (*Message, error) {

	requestID := uuid.New().String()
	message := Message{
		RequestID: requestID,
		Service:   service,
		Action:    action,
		Payload:   payload,
	}

	messageBytes, err := json.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	topic := fmt.Sprintf("%s_requests", service)
	kafkaMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(messageBytes),
	}

	// Send the message to Kafka
	partition, offset, err := kc.Producer.SendMessage(kafkaMessage)
	if err != nil {
		return nil, fmt.Errorf("failed to send message to Kafka: %w", err)
	}

	log.Printf("Message sent to partition %d at offset %d", partition, offset)

	// Prepare a response channel and store it using the RequestID
	responseChan := make(chan *Message, 1)
	kc.mux.Lock()
	kc.responseChans[requestID] = responseChan
	kc.mux.Unlock()

	// Wait for the response from the appropriate response topic
	select {
	case response := <-responseChan:
		return response, nil
	case <-time.After(10 * time.Second): // Timeout after 10 seconds
		// Remove the response channel after timeout to avoid memory leaks
		kc.mux.Lock()
		delete(kc.responseChans, requestID)
		kc.mux.Unlock()
		return nil, fmt.Errorf("timeout waiting for response for request ID %s", requestID)
	}
}

func (kc *Client) ConsumeResponses(service string) {
	topic := fmt.Sprintf("%s_responses", service)
	partitionConsumer, err := kc.Consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start consumer for topic %s: %v", topic, err)
	}

	defer partitionConsumer.Close()

	for message := range partitionConsumer.Messages() {
		var msg Message
		err := json.Unmarshal(message.Value, &msg)
		if err != nil {
			log.Printf("failed to unmarshal message: %v", err)
			continue
		}

		// Log the message for debugging
		log.Printf("Received response from Kafka: %+v", msg)

		// Check if there's a waiting channel for the response
		kc.mux.Lock()
		if responseChan, ok := kc.responseChans[msg.RequestID]; ok {
			// Send the response to the waiting channel
			responseChan <- &msg
			close(responseChan)
			delete(kc.responseChans, msg.RequestID)
		} else {
			log.Printf("No waiting response channel for RequestID %s", msg.RequestID)
		}
		kc.mux.Unlock()
	}
}

func (kc *Client) HandleUserResponse() {
	//topic := fmt.Sprintf("%s_responses", service)
	consumer, err := kc.Consumer.ConsumePartition("user_responses", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("failed to start consumer for user_responses topic: %v", err)
	}

	defer func(consumer sarama.PartitionConsumer) {
		err := consumer.Close()
		if err != nil {
			log.Fatalf("failed to close consumer for user_responses: %v", err)
		}
	}(consumer)

	for message := range consumer.Messages() {
		var msg Message
		err := json.Unmarshal(message.Value, &msg)
		if err != nil {
			log.Printf("failed to unmarshal message: %v", err)
			continue
		}

		log.Printf("Received response from user service: %+v", msg)
	}
}
