package kafkahook

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

// KafkaHook is a hook to handle writing to kafka log files.
type KafkaHook struct {
	formatter logrus.Formatter
	sync      bool
	topic     string
	client    sarama.Client
	levels    []logrus.Level
}

// NewHook returns new Kafka hook.
func NewHook(client sarama.Client, topic string, sync bool, formatter logrus.Formatter) *KafkaHook {
	hook := &KafkaHook{
		client:    client,
		sync:      sync,
		topic:     topic,
		formatter: formatter,
	}

	return hook
}

// Fire writes the log file to defined path or using the defined writer.
// User who run this function needs write permissions to the file or directory if the file does not yet exist.
func (hook *KafkaHook) Fire(entry *logrus.Entry) error {

	content := hook.createContent(entry)

	msg := &sarama.ProducerMessage{
		Topic: hook.topic,
		Key:   sarama.StringEncoder("go_log"),
	}
	msg.Value = sarama.ByteEncoder(content)
	if hook.sync {
		producer, err := sarama.NewSyncProducerFromClient(hook.client)
		if err != nil {
			return err
		}
		defer producer.Close()

		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			return err
		}
	} else {
		producer, err := sarama.NewAsyncProducerFromClient(hook.client)
		if err != nil {
			return err
		}
		defer producer.AsyncClose()
		producer.Input() <- msg
	}
	return nil
}

func (hook *KafkaHook) createContent(entry *logrus.Entry) []byte {
	// use our formatter instead of entry.String()
	msg, err := hook.formatter.Format(entry)
	if err != nil {
		return []byte("")
	}
	return msg
}

// Levels returns configured log levels.
func (hook *KafkaHook) Levels() []logrus.Level {
	return logrus.AllLevels
}
