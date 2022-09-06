package rabbit_reconnector

import (
	"errors"
	"time"

	_ "github.com/PCManiac/logrus_init"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Reconnector interface {
	RabbitReConnector()
	RabbitConnect() (err error)
	GetConnection() *amqp.Connection
	SubscribeTo(exhangeName string, queueName string, routingKey string) (<-chan amqp.Delivery, *amqp.Channel, error)
	PublishResponse(ch *amqp.Channel, exchangeName string, replyTo string, correlationId string, body string, sessionId string, content_type string) (err error)
	ExecuteRPC(ch *amqp.Channel, exhangeName string, body []byte, strTimeout string, routingKey string) (response []byte, err error)
	PublishTo(ch *amqp.Channel, exhangeName string, routingKey string, body string) (err error)
}

type ReconnectorEventHandler interface {
	AfterReconnect(AmqpConnection *amqp.Connection, ExitSignal chan bool) error
}

type server struct {
	AmqpConnection *amqp.Connection
	AmqpCloseError chan *amqp.Error
	ExitSignal     chan bool
	amqpHostName   string
	handler        ReconnectorEventHandler
}

func (s *server) RabbitConnect() (err error) {
	log.WithFields(log.Fields{
		"proc": "RabbitConnect",
	}).Warn("RabbitConnect started")

	var rabbit *amqp.Connection

	for i := 0; i < 60; i++ {
		rabbit, err = amqp.Dial(s.amqpHostName)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"stage": "dial",
				"proc":  "RabbitConnect",
			}).Error("AMQP error")

			time.Sleep(500 * time.Millisecond)
			continue
		}

		s.AmqpConnection = rabbit

		go func() {
			<-rabbit.NotifyClose(make(chan *amqp.Error))
			log.WithFields(log.Fields{
				"proc": "RabbitConnect goroutine",
			}).Error("NotifyClose received. Sending to reconnect.")
			s.AmqpCloseError <- amqp.ErrClosed
		}()

		if s.handler != nil {
			s.ExitSignal = make(chan bool, 1)
			if err := s.handler.AfterReconnect(s.AmqpConnection, s.ExitSignal); err != nil {
				log.WithFields(log.Fields{
					"error": err,
					"proc":  "RabbitConnect",
				}).Error("Failed to AfterReconnect")
				return err
			}
		}

		log.WithFields(log.Fields{
			"proc": "RabbitConnect",
		}).Warn("RabbitConnect Ok")
		return nil
	}

	log.WithFields(log.Fields{
		"proc": "RabbitConnect",
	}).Error("AMQP connect error")
	return errors.New("amqp connect error")
}

func (s *server) RabbitReConnector() {
	for {
		<-s.AmqpCloseError

		log.WithFields(log.Fields{
			"proc": "RabbitReConnector",
		}).Error("amqpCloseError received. Reconnecting.")

		s.ExitSignal <- true

		err := s.RabbitConnect()
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"stage": "RabbitConnect",
				"proc":  "RabbitReConnector",
			}).Panic("AMQP error")
		}
	}
}

func (s *server) GetConnection() *amqp.Connection {
	return s.AmqpConnection
}

func New(amqpHost string, handler ReconnectorEventHandler) (Reconnector, error) {
	s := server{
		AmqpCloseError: make(chan *amqp.Error),
		amqpHostName:   amqpHost,
		ExitSignal:     make(chan bool, 1),
	}

	if handler != nil {
		s.handler = handler
	}

	return &s, nil
}

func NewAndStart(amqpHost string, handler ReconnectorEventHandler) (Reconnector, error) {
	s := server{
		AmqpCloseError: make(chan *amqp.Error),
		amqpHostName:   amqpHost,
		ExitSignal:     make(chan bool, 1),
	}

	if handler != nil {
		s.handler = handler
	}

	go s.RabbitReConnector()

	err := s.RabbitConnect()
	if err != nil {
		return nil, err
	}

	return &s, nil
}
