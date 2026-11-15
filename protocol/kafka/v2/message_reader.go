package kafka

import (
	"bytes"
	"context"
	"strconv"
	"strings"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/format"
	"github.com/cloudevents/sdk-go/v2/binding/spec"
)

const (
	prefix         = "ce_"
	contentTypeKey = "content-type"
)

var specs = spec.WithPrefix(prefix)

const (
	KafkaOffsetKey    = "kafkaoffset"
	KafkaPartitionKey = "kafkapartition"
	KafkaTopicKey     = "kafkatopic"
	KafkaMessageKey   = "kafkamessagekey"
)

type KafkaHeader struct {
	Key   string
	Value []byte
}

type KafkaMessageIf interface {
	// Finish signals that the message has been processed and any resources can be released.
	Finish(error) error
	// Accessor methods for Kafka message metadata
	GetOffset() int64
	GetPartition() int32
	GetTopic() string
	// GetHeaders returns the Kafka message headers as a map.
	GetHeaders() []KafkaHeader
	// GetKey returns the Kafka message key.
	GetKey() []byte
	// GetValue returns the Kafka message value.
	GetValue() []byte
}

type MessageReaderIf interface {
	binding.Message
	binding.MessageMetadataReader
	KafkaMessageIf
}

func NewMessageReader(msg KafkaMessageIf) MessageReaderIf {
	if msg == nil {
		return nil
	}

	var contentType, contentVersion string
	headers := make(map[string][]byte, len(msg.GetHeaders())+3)
	for _, v := range msg.GetHeaders() {
		k := strings.ToLower(string(v.Key))
		if k == contentTypeKey {
			contentType = string(v.Value)
		}
		if k == specs.PrefixedSpecVersionName() {
			contentVersion = string(v.Value)
		}
		headers[k] = v.Value
	}

	// add the kafka message key, topic, partition and partition key to the properties
	headers[prefix+KafkaOffsetKey] = []byte(strconv.FormatInt(msg.GetOffset(), 10))
	headers[prefix+KafkaPartitionKey] = []byte(strconv.FormatInt(int64(msg.GetPartition()), 10))
	headers[prefix+KafkaTopicKey] = []byte(msg.GetTopic())
	// add the Kafka message key to the properties if it exists
	if msg.GetKey() != nil {
		headers[prefix+KafkaMessageKey] = msg.GetKey()
	}

	msgReader := &messageReader{
		KafkaMessageIf: msg,
		headers:        headers,
	}
	if ft := format.Lookup(contentType); ft != nil {
		msgReader.format = ft
	} else if v := specs.Version(contentVersion); v != nil {
		msgReader.version = v
	}

	return msgReader
}

// messageReader represents a Kafka message.
// This message *can* be read several times safely
type messageReader struct {
	KafkaMessageIf
	headers map[string][]byte
	format  format.Format
	version spec.Version
}

func (m *messageReader) Finish(err error) error {
	if m.KafkaMessageIf != nil {
		return m.KafkaMessageIf.Finish(err)
	}
	return nil
}

func (m *messageReader) ReadEncoding() binding.Encoding {
	if m.version != nil {
		return binding.EncodingBinary
	}
	if m.format != nil {
		return binding.EncodingStructured
	}
	return binding.EncodingUnknown
}

func (m *messageReader) ReadStructured(ctx context.Context, encoder binding.StructuredWriter) error {
	if m.format != nil {
		return encoder.SetStructuredEvent(ctx, m.format, bytes.NewReader(m.GetValue()))
	}
	return binding.ErrNotStructured
}

func (m *messageReader) ReadBinary(ctx context.Context, encoder binding.BinaryWriter) error {
	if m.version == nil {
		return binding.ErrNotBinary
	}

	// Set the attributes from the Kafka headers into the binary encoder.
	var err error
	for k, v := range m.headers {
		if strings.HasPrefix(k, prefix) {
			attr := m.version.Attribute(k)
			if attr != nil {
				err = encoder.SetAttribute(attr, string(v))
			} else {
				err = encoder.SetExtension(strings.TrimPrefix(k, prefix), string(v))
			}
		} else if k == contentTypeKey {
			err = encoder.SetAttribute(m.version.AttributeFromKind(spec.DataContentType), string(v))
		}
		if err != nil {
			return err
		}
	}

	if m.GetValue() != nil {
		err = encoder.SetData(bytes.NewBuffer(m.GetValue()))
	}

	return err
}

func (m *messageReader) GetAttribute(k spec.Kind) (spec.Attribute, interface{}) {
	if attr := m.version.AttributeFromKind(k); attr != nil {
		return attr, m.headers[attr.PrefixedName()]
	}
	return nil, nil
}

func (m *messageReader) GetExtension(name string) interface{} {
	return m.headers[prefix+name]
}
