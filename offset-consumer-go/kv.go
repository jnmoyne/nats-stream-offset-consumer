package offsetconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	kvBucketName       = "offset-consumers"
	kvStreamNamePrefix = "KV_"
	defaultAPIPrefix   = "$JS.API."

	// maxCombinedNameLen bounds len(stream)+len(consumerName). Ordered
	// consumer names are "oc-{stream}-{consumer}-{22-char nuid}-g{gen}" plus
	// a client suffix, and the server limits names to 255 characters; 32
	// characters are reserved for the generation and client suffixes.
	maxCombinedNameLen = 255 - 32 - len("oc-") - 2 - 22
)

// validName matches names usable both in a KV key token and in a consumer name.
var validName = regexp.MustCompile(`^[A-Za-z0-9_=-]+$`)

// validateNames checks the stream and consumer names.
func validateNames(stream, consumerName string) error {
	if !validName.MatchString(consumerName) {
		return fmt.Errorf("%w %q: use letters, digits, '-', '_' and '=' only", ErrInvalidConsumerName, consumerName)
	}
	if !validName.MatchString(stream) {
		return fmt.Errorf("%w %q: use letters, digits, '-', '_' and '=' only", ErrInvalidStreamName, stream)
	}
	if len(stream)+len(consumerName) > maxCombinedNameLen {
		return fmt.Errorf("%w %q: stream and consumer names together exceed %d characters", ErrInvalidConsumerName, consumerName, maxCombinedNameLen)
	}
	return nil
}

// streamCreatedID identifies a stream incarnation by its creation time, as
// decimal Unix nanoseconds (the same string in every port).
func streamCreatedID(created time.Time) string {
	if created.IsZero() {
		return ""
	}
	return strconv.FormatInt(created.UnixNano(), 10)
}

// streamWasRecreated reports whether a stored entry refers to an earlier
// incarnation of the stream. Entries from older versions carry no stream
// identity; for those a stored sequence beyond the stream's last sequence is
// the tell-tale sign.
func streamWasRecreated(entry offsetEntry, currentCreated string, lastSeq uint64) bool {
	if entry.StreamCreated != "" && currentCreated != "" {
		return entry.StreamCreated != currentCreated
	}
	return entry.Seq > lastSeq
}

// errClaimConflict is returned by claimEntry when the CAS write loses a race
// against another instance claiming the same entry.
var errClaimConflict = errors.New("offset consumer: claim conflict")

// offsetEntry is the persisted record in the offset-consumers KV bucket.
type offsetEntry struct {
	// Seq is the last committed stream sequence.
	Seq uint64 `json:"seq"`
	// CommittedAt is the RFC3339 time of the last commit.
	CommittedAt string `json:"committed_at"`
	// Owner is the owner prefix of the live instance holding this entry.
	// Empty when the entry is not owned (released, or never claimed).
	Owner string `json:"owner"`
	// Host is the hostname of the owning instance.
	Host string `json:"host"`
	// StreamCreated is the stream's creation time in Unix nanoseconds
	// (decimal string): the stream incarnation Seq refers to.
	StreamCreated string `json:"stream_created,omitempty"`
}

func (e offsetEntry) info() OffsetConsumerInfo {
	return OffsetConsumerInfo{
		Seq:           e.Seq,
		CommittedAt:   e.CommittedAt,
		Owner:         e.Owner,
		Host:          e.Host,
		StreamCreated: e.StreamCreated,
	}
}

func marshalEntry(e offsetEntry) ([]byte, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("offset consumer: failed to marshal offset entry: %w", err)
	}
	return data, nil
}

func unmarshalEntry(data []byte) (offsetEntry, error) {
	var entry offsetEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return offsetEntry{}, fmt.Errorf("offset consumer: failed to parse offset entry: %w", err)
	}
	return entry, nil
}

func rfc3339Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// readEntry reads and parses the offset entry. found is false when the key
// does not exist or has been deleted.
func readEntry(ctx context.Context, kv jetstream.KeyValue, key string) (entry offsetEntry, revision uint64, found bool, err error) {
	kve, err := kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return offsetEntry{}, 0, false, nil
		}
		return offsetEntry{}, 0, false, fmt.Errorf("offset consumer: failed to read offset entry: %w", err)
	}
	entry, err = unmarshalEntry(kve.Value())
	if err != nil {
		return offsetEntry{}, 0, false, err
	}
	return entry, kve.Revision(), true, nil
}

// claimEntry writes entry with a compare-and-set against revision (0 means
// the key must not exist). Returns errClaimConflict if another writer got
// there first.
func claimEntry(ctx context.Context, kv jetstream.KeyValue, key string, entry offsetEntry, revision uint64) (uint64, error) {
	data, err := marshalEntry(entry)
	if err != nil {
		return 0, err
	}
	var rev uint64
	if revision == 0 {
		rev, err = kv.Create(ctx, key, data)
	} else {
		rev, err = kv.Update(ctx, key, data, revision)
	}
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return 0, fmt.Errorf("%w: %w", errClaimConflict, err)
		}
		return 0, fmt.Errorf("offset consumer: failed to claim offset entry: %w", err)
	}
	return rev, nil
}

// isOwnerAlive reports whether the stream has a consumer whose name starts
// with owner, i.e. whether the owning instance's ordered consumer still exists.
func isOwnerAlive(ctx context.Context, stream jetstream.Stream, owner string) (bool, error) {
	lister := stream.ConsumerNames(ctx)
	alive := false
	for name := range lister.Name() {
		if strings.HasPrefix(name, owner) {
			alive = true
		}
	}
	if err := lister.Err(); err != nil {
		return false, fmt.Errorf("offset consumer: failed to list consumers: %w", err)
	}
	return alive, nil
}

// kvPutSubject returns the subject a KV put for key is published on, derived
// the same way nats.go derives it (JetStream API prefix or domain, and the
// external API prefix of a mirrored bucket). It is used for async commits,
// which publish directly instead of going through KeyValue.Put.
func kvPutSubject(ctx context.Context, js jetstream.JetStream, kv jetstream.KeyValue, key string) string {
	opts := js.Options()
	apiPrefix := defaultAPIPrefix
	if opts.Domain != "" {
		apiPrefix = "$JS." + opts.Domain + ".API."
	} else if opts.APIPrefix != "" {
		apiPrefix = opts.APIPrefix
		if !strings.HasSuffix(apiPrefix, ".") {
			apiPrefix += "."
		}
	}
	useJSPrefix := apiPrefix != defaultAPIPrefix

	putPrefix := "$KV." + kv.Bucket() + "."
	if status, err := kv.Status(ctx); err == nil {
		if bs, ok := status.(*jetstream.KeyValueBucketStatus); ok && bs.StreamInfo() != nil {
			if m := bs.StreamInfo().Config.Mirror; m != nil {
				bucket := strings.TrimPrefix(m.Name, kvStreamNamePrefix)
				if m.External != nil && m.External.APIPrefix != "" {
					useJSPrefix = false
					putPrefix = m.External.APIPrefix + ".$KV." + bucket + "."
				} else {
					putPrefix = "$KV." + bucket + "."
				}
			}
		}
	}

	var b strings.Builder
	if useJSPrefix {
		b.WriteString(apiPrefix)
	}
	b.WriteString(putPrefix)
	b.WriteString(key)
	return b.String()
}

// ensureBucket returns the offset-consumers KV bucket, creating it if it
// does not exist with replicas matching the target stream, history=1, and a
// 10-minute delete marker TTL. An existing bucket's configuration is never
// overwritten; its replica count is only ever raised (best effort) so that
// offsets of a replicated stream are not stored with fewer replicas than the
// stream itself.
func ensureBucket(ctx context.Context, js jetstream.JetStream, replicas int) (jetstream.KeyValue, error) {
	if replicas < 1 {
		replicas = 1
	}

	kv, err := js.KeyValue(ctx, kvBucketName)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:         kvBucketName,
			Replicas:       replicas,
			History:        1,
			LimitMarkerTTL: 10 * time.Minute,
		})
		if errors.Is(err, jetstream.ErrBucketExists) {
			// Created concurrently, possibly with another configuration.
			kv, err = js.KeyValue(ctx, kvBucketName)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("offset consumer: failed to open KV bucket: %w", err)
	}

	raiseBucketReplicas(ctx, js, replicas)
	return kv, nil
}

// raiseBucketReplicas raises the bucket's replica count to replicas if it is
// lower. Errors (permissions, too few servers) are ignored: the bucket keeps
// working with its current replicas.
func raiseBucketReplicas(ctx context.Context, js jetstream.JetStream, replicas int) {
	stream, err := js.Stream(ctx, kvStreamNamePrefix+kvBucketName)
	if err != nil {
		return
	}
	cfg := stream.CachedInfo().Config
	if cfg.Replicas >= replicas {
		return
	}
	cfg.Replicas = replicas
	_, _ = js.UpdateStream(ctx, cfg)
}

// ListConsumers returns all offset consumers for the given stream by using
// KV's filtered key listing with a "{stream}.*" filter pattern.
// Returns a map of consumer name to OffsetConsumerInfo.
// Returns an empty map if no consumers exist for the stream.
func ListConsumers(ctx context.Context, js jetstream.JetStream, stream string) (map[string]OffsetConsumerInfo, error) {
	result := make(map[string]OffsetConsumerInfo)

	kv, err := js.KeyValue(ctx, kvBucketName)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return result, nil
		}
		return nil, fmt.Errorf("offset consumer: failed to open KV bucket: %w", err)
	}

	prefix := stream + "."
	lister, err := kv.ListKeysFiltered(ctx, stream+".*")
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return result, nil
		}
		return nil, fmt.Errorf("offset consumer: failed to list keys: %w", err)
	}

	for key := range lister.Keys() {
		consumerName := strings.TrimPrefix(key, prefix)
		entry, _, found, err := readEntry(ctx, kv, key)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		result[consumerName] = entry.info()
	}

	return result, nil
}
