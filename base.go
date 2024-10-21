package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"log"
	"sync"
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
	responseChans map[string]chan *Message
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

func (kc *Client) SendMessage(service, action, payload string) (string, error) {
	requestID := uuid.New().String()
	message := Message{
		RequestID: requestID,
		Service:   service,
		Action:    action,
		Payload:   payload,
	}

	msgBt, err := json.Marshal(message)
	if err != nil {
		return "", fmt.Errorf("failed to marshal message: %w", err)
	}

	topic := fmt.Sprintf("%s_requests", service)

	kmsg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(msgBt),
	}

	partition, offset, err := kc.Producer.SendMessage(kmsg)
	if err != nil {
		return "", err
	}
	log.Printf("Message sent to partition %d at offset %d\n", partition, offset)

	return requestID, nil
}

func (kc *Client) Consume(topic string, actionHandlers map[string]func(*Message) *Message) {
	partitionConsumer, err := kc.Consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start consumer for topic %s: %v", topic, err)
	}

	defer func(partitionConsumer sarama.PartitionConsumer) {
		err := partitionConsumer.Close()
		if err != nil {
			log.Fatalf("Failed to close partition consumer for topic %s: %v", topic, err)
		}
	}(partitionConsumer)

	for message := range partitionConsumer.Messages() {
		var msg Message
		err := json.Unmarshal(message.Value, &msg)
		if err != nil {
			log.Printf("Failed to unmarshal message for topic %s: %v", topic, err)
			continue
		}

		// Log the received message
		log.Printf("Received message from Kafka: %+v", msg)

		// Find the handler for the specific action
		if handler, ok := actionHandlers[msg.Action]; ok {
			response := handler(&msg)
			if response != nil {
				responseBytes, err := json.Marshal(response)
				if err != nil {
					log.Printf("Failed to marshal response message: %v", err)
					continue
				}

				responseTopic := fmt.Sprintf("%s_responses", msg.Service)
				kafkaMessage := &sarama.ProducerMessage{
					Topic: responseTopic,
					Value: sarama.ByteEncoder(responseBytes),
				}

				_, _, err = kc.Producer.SendMessage(kafkaMessage)
				if err != nil {
					log.Printf("Failed to send response to Kafka: %v", err)
				}
			}
		} else {
			log.Printf("No handler found for action: %s", msg.Action)
		}
	}
}

func (kc *Client) ConsumeResponses(serviceHandlers map[string]func(*Message)) {
	for service, handler := range serviceHandlers {
		go func(service string, handler func(*Message)) {
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
					log.Printf("Failed to unmarshal message for topic %s: %v", topic, err)
					continue
				}

				// Log the received response
				log.Printf("Received response from Kafka: %+v", msg)

				// Call the handler for this message
				handler(&msg)
			}
		}(service, handler)
	}
}
