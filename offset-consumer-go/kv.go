package offsetconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const kvBucketName = "offset-consumers"

// offsetEntry is the persisted record in the offset-consumers KV bucket.
type offsetEntry struct {
	Seq         uint64 `json:"seq"`
	CommittedAt string `json:"committed_at"`
}

func marshalOffset(seq uint64) ([]byte, error) {
	entry := offsetEntry{
		Seq:         seq,
		CommittedAt: time.Now().UTC().Format(time.RFC3339),
	}
	return json.Marshal(entry)
}

func unmarshalOffset(data []byte) (offsetEntry, error) {
	var entry offsetEntry
	err := json.Unmarshal(data, &entry)
	return entry, err
}

// ensureBucket creates the offset-consumers KV bucket if it does not exist,
// or returns the existing one. The bucket is created with replicas matching
// the target stream, history=1, and a 10-minute delete marker TTL.
func ensureBucket(ctx context.Context, js jetstream.JetStream, replicas int) (jetstream.KeyValue, error) {
	if replicas < 1 {
		replicas = 1
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:       kvBucketName,
		Replicas:     replicas,
		History:      1,
		LimitMarkerTTL: 10 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("offset consumer: failed to ensure KV bucket: %w", err)
	}
	return kv, nil
}

// kvUpdate performs a CAS update on the KV store. If lastRevision is 0, it
// creates the key; otherwise it updates with the expected revision.
// Returns the new revision on success, or ErrCASConflict on revision mismatch.
func kvUpdate(ctx context.Context, kv jetstream.KeyValue, key string, value []byte, lastRevision uint64) (uint64, error) {
	var rev uint64
	var err error

	if lastRevision == 0 {
		rev, err = kv.Create(ctx, key, value)
	} else {
		rev, err = kv.Update(ctx, key, value, lastRevision)
	}

	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return 0, fmt.Errorf("%w: %w", ErrCASConflict, err)
		}
		return 0, fmt.Errorf("offset consumer: KV write failed: %w", err)
	}
	return rev, nil
}
