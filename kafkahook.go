package kafkahook

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

// KafkaHook is a hook to handle writing to kafka log files.
type KafkaHook struct {
	formatter      logrus.Formatter
	sync           bool
	topic          string
	accessTopic    string
	isAccessLogKey string
	syncProducer   sarama.SyncProducer
	asyncProducer  sarama.AsyncProducer
	levels         []logrus.Level
	timeout        time.Duration
}

type Option func(hook *KafkaHook)

func WithTimeout(timeout time.Duration) Option {
	return func(hook *KafkaHook) {
		hook.timeout = timeout
	}
}

func WithAccessLogTopic(accessTopic string) Option {
	return func(hook *KafkaHook) {
		if accessTopic != "" {
			hook.accessTopic = accessTopic
		}
	}
}

func WithIsAccessLogKey(isAccessLogKey string) Option {
	return func(hook *KafkaHook) {
		hook.isAccessLogKey = isAccessLogKey
	}
}

func WithLevels(levels []logrus.Level) Option {
	return func(hook *KafkaHook) {
		hook.levels = levels
	}
}

func WithFormatter(formatter logrus.Formatter) Option {
	return func(hook *KafkaHook) {
		hook.formatter = formatter
	}
}

func newDefaultKafkaHook(topic string) *KafkaHook {
	hook := &KafkaHook{
		formatter:     &logrus.TextFormatter{},
		sync:          true,
		topic:         topic,
		accessTopic:   topic,
		syncProducer:  nil,
		asyncProducer: nil,
		levels:        nil,
	}
	hook.topic = topic
	hook.accessTopic = topic
	hook.isAccessLogKey = "is_access_log"
	hook.sync = true
	return hook
}

// NewHook returns new Kafka hook.
func NewSyncHook(topic string, producer sarama.SyncProducer, opts ...Option) *KafkaHook {
	hook := newDefaultKafkaHook(topic)
	hook.syncProducer = producer
	for _, o := range opts {
		o(hook)
	}
	return hook
}

// NewHook returns new Kafka hook.
func NewAsyncHook(topic string, producer sarama.AsyncProducer, opts ...Option) *KafkaHook {
	hook := newDefaultKafkaHook(topic)
	hook.sync = false
	hook.asyncProducer = producer
	for _, o := range opts {
		o(hook)
	}
	return hook
}

// Fire writes the log file to defined path or using the defined writer.
// User who run this function needs write permissions to the file or directory if the file does not yet exist.
func (hook *KafkaHook) Fire(entry *logrus.Entry) error {
	defer func() {
		if r := recover(); r != nil {
		}
	}()
	content := hook.createContent(entry)
	topic := hook.getTopic(entry)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder("go_log"),
	}
	msg.Value = sarama.ByteEncoder(content)
	if hook.sync {
		_, _, err := hook.syncProducer.SendMessage(msg)
		if err != nil {
			return err
		}
	} else {
		if hook.timeout > 0 {
			select {
			case <-time.After(hook.timeout):
			case hook.asyncProducer.Input() <- msg:
			}
		} else {
			hook.asyncProducer.Input() <- msg
		}
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

func (hook *KafkaHook) getTopic(entry *logrus.Entry) string {
	topic := hook.topic
	fields := entry.Data
	if len(fields) > 0 {
		for fk, fv := range fields {
			if fk == hook.isAccessLogKey {
				if fv.(bool) {
					topic = hook.accessTopic
				}
				break
			}
		}
	}

	return topic
}

// Levels returns configured log levels.
func (hook *KafkaHook) Levels() []logrus.Level {
	return logrus.AllLevels
}
