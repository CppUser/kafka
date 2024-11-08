package kafka

import (
	"context"
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
	ConsumerGroup sarama.ConsumerGroup
	waitGroup     sync.WaitGroup
	responseChans map[string]chan *Message
	mux           sync.Mutex
}

// NewClient instance
func NewClient(brokers []string, groupID string) (*Client, error) {
	log.Printf("Creating new kafka client")
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true
	config.Version = sarama.V3_6_0_0

	// Assign unique consumer group ID
	config.Consumer.Group.InstanceId = groupID

	// Use the updated GroupStrategies with the new functions to avoid deprecated methods
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRange(),
		sarama.NewBalanceStrategyRoundRobin(),
	}

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer group: %w", err)
	}

	return &Client{
		Producer:      producer,
		ConsumerGroup: consumer,
		responseChans: make(map[string]chan *Message),
	}, nil
}

func (kc *Client) Close() error {
	if err := kc.Producer.Close(); err != nil {
		return fmt.Errorf("failed to close producer: %w", err)
	}
	if err := kc.ConsumerGroup.Close(); err != nil {
		return fmt.Errorf("failed to close consumer: %w", err)
	}
	return nil
}

/////////////////////////REFACTOR MOVE TO ITS OWN FILES////////////////////////////////////

// SendMessage sends a message to a specific Kafka topic
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

type consumerGroupHandler struct {
	client          *Client
	actionHandlers  map[string]func(*Message) *Message
	responseHandler func(*Message)
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var msg Message
		err := json.Unmarshal(message.Value, &msg)
		if err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			continue
		}

		log.Printf("Received message from Kafka: %+v", msg)

		// Decide based on actionHandlers or responseHandler
		if h.actionHandlers != nil {
			if handler, ok := h.actionHandlers[msg.Action]; ok {
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

					_, _, err = h.client.Producer.SendMessage(kafkaMessage)
					if err != nil {
						log.Printf("Failed to send response to Kafka: %v", err)
					}
				}
			} else {
				log.Printf("No handler found for action: %s", msg.Action)
			}
		} else if h.responseHandler != nil {
			h.responseHandler(&msg)
		}

		session.MarkMessage(message, "")
	}
	return nil
}

// Consume consumes messages on a topic using specific action handlers
func (kc *Client) Consume(topic string, actionHandlers map[string]func(*Message) *Message) {
	handler := &consumerGroupHandler{
		client:         kc,
		actionHandlers: actionHandlers,
	}

	go func() {
		for {
			if err := kc.ConsumerGroup.Consume(context.Background(), []string{topic}, handler); err != nil {
				log.Printf("Error from consumer group: %v", err)
			}
		}
	}()
}

func (kc *Client) ConsumeResponses(serviceHandlers map[string]func(*Message)) {
	for service, handler := range serviceHandlers {
		topic := fmt.Sprintf("%s_responses", service)

		go func() {
			responseHandler := &consumerGroupHandler{
				client:          kc,
				responseHandler: handler,
			}

			for {
				if err := kc.ConsumerGroup.Consume(context.Background(), []string{topic}, responseHandler); err != nil {
					log.Printf("Error from consumer group: %v", err)
				}
			}
		}()
	}
}

// SetResponseChan sets a response channel for a specific request ID
func (kc *Client) SetResponseChan(requestID string, responseChan chan *Message) {
	kc.mux.Lock()
	defer kc.mux.Unlock()
	kc.responseChans[requestID] = responseChan
}

// GetResponseChan retrieves a response channel for a given request ID
func (kc *Client) GetResponseChan(requestID string) (chan *Message, bool) {
	kc.mux.Lock()
	defer kc.mux.Unlock()
	responseChan, exists := kc.responseChans[requestID]
	return responseChan, exists
}

// DeleteResponseChan removes a response channel for a given request ID
func (kc *Client) DeleteResponseChan(requestID string) {
	kc.mux.Lock()
	defer kc.mux.Unlock()
	delete(kc.responseChans, requestID)
}
