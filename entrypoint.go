package natr

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"time"
	// "log"
)

// attributes = {
//   "eventTime" : "2019-03-22T18:41:50.201712Z"
//   "eventType" : "OBJECT_DELETE"
//   "notificationConfig" : "projects/_/buckets/some-bucket/notificationConfigs/1"
//   "objectGeneration" : "1234567890123456"
//   "objectId" : "test.file"
//   "payloadFormat" : "JSON_API_V1"
//   "bucketId" : "some-bucket"
// }

// objectAttrs = {
//   "kind": "storage#object",
//   "id": "some-bucket/test.file/1234567890123456",
//   "selfLink": "https://www.googleapis.com/storage/v1/b/some-bucket/o/test.file",
//   "name": "test.file",
//   "bucket": "some-bucket",
//   "generation": "1234567890123456",
//   "metageneration": "1",
//   "contentType": "application/octet-stream",
//   "timeCreated": "2019-03-22T12:34:56.123Z",
//   "updated": "2019-03-22T12:34:56.123Z",
//   "storageClass": "STANDARD",
//   "timeStorageClassUpdated": "2019-03-22T12:34:56.123Z",
//   "size": "912",
//   "md5Hash": "AAbbcchddeeffggl+/N4NA==",
//   "mediaLink": "https://www.googleapis.com/download/storage/v1/b/some-bucket/o/test.file?generation=1234567890123456&alt=media",
//   "contentLanguage": "en",
//   "crc32c": "ABCDEF==",
//   "etag": "CL/q+ZmzluECEAE="
// }

type GCSPubSubMessage struct {
	Data       string            `json:"data"`
	Attributes GCSEventAttribute `json:"attributes"`
}

type GCSEventAttribute struct {
	NotificationConfig string    `json:"notificationConfig"`
	EventType          string    `json:"eventType"`
	EventTime          time.Time `json:"eventTime"`
	PayloadFormat      string    `json:"payloadFormat"`
	BucketId           string    `json:"bucketId"`
	ObjectId           string    `json:"objectId"`
	ObjectGeneration   string    `json:"objectGeneration"`
}

// almost of all fields are same as storage.ObjectAttrs from "cloud.google.com/go/storage"
// but `generation` field in parameter is string, not int64
type GCSObjectAttrs struct {
	Kind                    string    `json:"kind"`
	ID                      string    `json:"id"`
	SelfLink                string    `json:"selfLink"`
	Name                    string    `json:"name"`
	Bucket                  string    `json:"bucket"`
	Generation              string    `json:"generation"`
	Metageneration          string    `json:"metageneration"`
	ContentType             string    `json:"contentType"`
	TimeCreated             time.Time `json:"timeCreated"`
	Updated                 time.Time `json:"updated"`
	StorageClass            string    `json:"storageClass"`
	TimeStorageClassUpdated string    `json:"timeStorageClassUpdated"`
	Size                    string    `json:"size"`
	Md5Hash                 string    `json:"md5Hash"`
	MediaLink               string    `json:"mediaLink"`
	Crc32c                  string    `json:"crc32c"`
	ContentLanguage         string    `json:"contentLanguage"`
	Etag                    string    `json:"etag"`
}

// PubSub
// gcloud functions deploy gcs-notification-func --trigger-topic gcs-notification --runtime go111 --entry-point OnGCSPubSubMessage
func OnGCSPubSubMessage(ctx context.Context, message GCSPubSubMessage) error {
	// meta, err := metadata.FromContext(ctx)
	// if err != nil {
	// 	return fmt.Errorf("metadata.FromContext: %v", err)
	// }

	// var err error
	// m.Data
	decoder := base64.NewDecoder(base64.StdEncoding, bytes.NewReader([]byte(message.Data)))
	gcsObjectJson, err := ioutil.ReadAll(decoder)
	if err != nil {
		return fmt.Errorf("ioutil.ReadAll: %v", err)
	}

	var objectAttrs GCSObjectAttrs
	if err := json.Unmarshal(gcsObjectJson, &objectAttrs); err != nil {
		return fmt.Errorf("json.Unmarshal: %v", err)
	}

	// log.Printf("meta: %+v", meta)
	log.Printf("EventType: %+v", message.Attributes.EventType)
	log.Printf("objectAttrs: %+v", objectAttrs)

	return nil
}

// gcloud functions deploy gcs-trigger-func --trigger-resource some-bucket --trigger-event google.storage.object.finalize --runtime go111 --entry-point OnGCSTrigger
func OnGCSTrigger(ctx context.Context, message GCSObjectAttrs) error {
	log.Printf("objectAttrs: %+v", message)

	return nil
}
