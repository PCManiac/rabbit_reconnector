package rabbit_reconnector

import (
	"errors"
	"time"

	_ "github.com/PCManiac/logrus_init"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func (s *server) SubscribeTo(exhangeName string, queueName string, routingKey string) (<-chan amqp.Delivery, *amqp.Channel, error) {
	ch, err := s.AmqpConnection.Channel()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"stage": "Channel create",
			"proc":  "RabbitConnect",
		}).Error("AMQP error")
		return nil, nil, err
	}

	err = ch.ExchangeDeclare(
		exhangeName, // name
		"direct",    // type
		true,        // durable
		true,        // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"exchange": exhangeName,
			"stage":    "exchange declare",
		}).Error("AMQP error.")
		return nil, nil, err
	}

	q, err := ch.QueueDeclare(
		queueName,       // name
		true,            // durable
		true,            // delete when unused
		queueName == "", // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"exchange": exhangeName,
			"stage":    "queue declare",
		}).Error("AMQP error.")
		return nil, nil, err
	}
	err = ch.QueueBind(
		q.Name,      // queue name
		routingKey,  // routing key
		exhangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"exchange": exhangeName,
			"stage":    "bind",
		}).Error("AMQP error.")
		return nil, nil, err
	}
	msgs, err := ch.Consume(
		q.Name,          // queue
		"",              // consumer
		true,            // auto-ack
		queueName == "", // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"exchange": exhangeName,
			"stage":    "consume",
		}).Error("AMQP error.")
		return nil, nil, err
	}
	return msgs, ch, nil
}

func (s *server) PublishResponse(ch *amqp.Channel, exchangeName string, replyTo string, correlationId string, body string, sessionId string, content_type string) (err error) {
	err1 := ch.Publish(
		exchangeName, // exchange
		replyTo,      // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:   content_type,
			CorrelationId: correlationId,
			Body:          []byte(body),
		})

	if err1 != nil {
		log.WithFields(log.Fields{
			"error":      err1,
			"session_id": sessionId,
			"proc":       "publishResponse",
		}).Error("Failed to publish to RPC")
		return err
	} else {
		log.WithFields(log.Fields{
			"body":       body,
			"session_id": sessionId,
			"proc":       "publishResponse",
			"exchange":   exchangeName,
		}).Trace("Published to RPC")
		return nil
	}
}

func (s *server) ExecuteRPC(ch *amqp.Channel, exhangeName string, body []byte, strTimeout string, routingKey string) (response []byte, err error) {
	err = ch.ExchangeDeclare(
		exhangeName, // name
		"direct",    // type
		true,        // durable
		true,        // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"exchange": exhangeName,
			"stage":    "exchange declare",
			"proc":     "executeRPC",
		}).Error("AMQP error.")
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"",    // name
		true,  // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"exchange": exhangeName,
			"stage":    "queue declare",
			"proc":     "executeRPC",
		}).Error("AMQP error.")
		return nil, err
	}

	err = ch.QueueBind(
		q.Name,      // queue name
		q.Name,      // routing key
		exhangeName, // exchange
		false,
		nil)
	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"exchange": exhangeName,
			"stage":    "bind",
			"proc":     "executeRPC",
		}).Error("AMQP error.")
		return nil, err
	}

	corrID := uuid.NewString()

	err = ch.Publish(
		exhangeName, // exchange
		routingKey,  // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrID,
			ReplyTo:       q.Name,
			Body:          body,
		})

	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"exchange": exhangeName,
			"stage":    "publish",
			"proc":     "executeRPC",
		}).Error("AMQP error.")
		return nil, err
	} else {
		log.WithFields(log.Fields{
			"body":     body,
			"exchange": exhangeName,
			"stage":    "publish",
			"proc":     "executeRPC",
		}).Trace("RPC published")
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		true,   // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"exchange": exhangeName,
			"stage":    "consume",
			"proc":     "executeRPC",
		}).Error("AMQP error.")
		return nil, err
	}

	timeout_interval, errt := time.ParseDuration(strTimeout)
	if errt != nil {
		timeout_interval, _ = time.ParseDuration("10s")
		log.WithFields(log.Fields{
			"config_timeout": strTimeout,
			"proc":           "executeRPC",
		}).Warn("Failed to parse timeout")
	}

	timeout := time.After(timeout_interval)
	for {
		select {
		case d := <-msgs:
			if corrID == d.CorrelationId {
				res := string(d.Body)

				log.WithFields(log.Fields{
					"response": res,
					"exchange": exhangeName,
					"stage":    "for loop",
					"proc":     "executeRPC",
				}).Trace("AMQP RPC response")

				return d.Body, nil
			}
			return
		case <-timeout:
			log.WithFields(log.Fields{
				"error":    "AMQP RPC timeout",
				"exchange": exhangeName,
				"stage":    "for loop",
				"proc":     "executeRPC",
			}).Error("AMQP timeout")
			return nil, errors.New("timeout")
		}
	}
}

func (s *server) PublishTo(ch *amqp.Channel, exhangeName string, routingKey string, body string) (err error) {
	err = ch.ExchangeDeclare(
		exhangeName, // name
		"direct",    // type
		true,        // durable
		true,        // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"exchange": exhangeName,
			"stage":    "exchange declare",
		}).Error("AMQP error.")
		return err
	}

	err = ch.Publish(
		exhangeName, // exchange
		routingKey,  // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})

	if err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"exchange": exhangeName,
			"stage":    "publish",
		}).Error("AMQP error.")
		return err
	} else {
		log.WithFields(log.Fields{
			"body":     body,
			"exchange": exhangeName,
			"stage":    "publish",
		}).Trace("Data published")
	}

	return nil
}
