package monitoring

import (
	"fmt"

	"github.com/jamespfennell/hoard/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var taskNumCompletions *prometheus.CounterVec
var taskLastCompletion *prometheus.GaugeVec

var downloadCount *prometheus.CounterVec
var downloadFailedCount *prometheus.CounterVec
var downloadSavedCount *prometheus.CounterVec
var downloadSavedSize *prometheus.CounterVec
var packCount *prometheus.CounterVec
var packFailedCount *prometheus.CounterVec
var packUnpackedSize *prometheus.CounterVec
var packPackedSize *prometheus.CounterVec
var packFileErrors *prometheus.CounterVec
var uploadCount *prometheus.CounterVec
var uploadFailedCount *prometheus.CounterVec
var auditFailedCount *prometheus.CounterVec
var localFilesCount *prometheus.GaugeVec
var localFilesSize *prometheus.GaugeVec
var remoteStorageDownloadCount *prometheus.CounterVec
var remoteStorageDownloadError *prometheus.CounterVec
var remoteStorageDownloadSize *prometheus.CounterVec
var remoteStorageUploadCount *prometheus.CounterVec
var remoteStorageUploadError *prometheus.CounterVec
var remoteStorageUploadSize *prometheus.CounterVec
var remoteStorageObjectsCount *prometheus.GaugeVec
var remoteStorageObjectsSize *prometheus.GaugeVec

func init() {
	taskNumCompletions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_task_num_completions",
			Help: "Number of times a task has completed for a specific feed",
		},
		[]string{"task", "feed_id", "success"},
	)
	taskLastCompletion = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hoard_task_last_completion",
			Help: "Last time a task completed for a particular feed, as a Unix timestamp",
		},
		[]string{"task", "feed_id", "success"},
	)
	downloadCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_download_count",
			Help: "Number of times an attempt has been made to download a feed",
		},
		[]string{"feed_id"},
	)
	downloadFailedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_download_failed_count",
			Help: "Number of times a feed download attempt failed",
		},
		[]string{"feed_id"},
	)
	downloadSavedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_download_saved_count",
			Help: "Number of successful downloads of a feed",
		},
		[]string{"feed_id"},
	)
	downloadSavedSize = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_download_saved_size",
			Help: "Total size of all saved feed downloads",
		},
		[]string{"feed_id"},
	)
	packCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_pack_count",
			Help: "Number of times a pack operation has occurred for each feed",
		},
		[]string{"feed_id"},
	)
	packFailedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_pack_failed_count",
			Help: "Number of times a pack operation has failed",
		},
		[]string{"feed_id"},
	)
	packUnpackedSize = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_pack_unpacked_size",
			Help: "Total size of all data before being packed",
		},
		[]string{"feed_id"},
	)
	packPackedSize = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_pack_packed_size",
			Help: "Total size of all data after being packed",
		},
		[]string{"feed_id"},
	)
	packFileErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_pack_file_errors",
			Help: "Number of file errors encountered when packing, including file deletion errors",
		},
		[]string{"feed_id"},
	)
	uploadCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_upload_count",
			Help: "Number of times an upload has occurred for each feed",
		},
		[]string{"feed_id"},
	)
	uploadFailedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_upload_failed_count",
			Help: "Number of failed uploads for each feed",
		},
		[]string{"feed_id"},
	)
	localFilesCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hoard_local_files_count",
			Help: "Number of files currently on local disk",
		},
		[]string{"directory", "feed_id"},
	)
	auditFailedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_audit_failed_count",
			Help: "Number of failed audits",
		},
		[]string{"feed_id"},
	)
	localFilesSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hoard_local_files_size",
			Help: "Total size of all files currently on local disk",
		},
		[]string{"directory", "feed_id"},
	)
	remoteStorageDownloadCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_remote_storage_download_count",
			Help: "Number of times a remote archive file has been downloaded to local disk",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageDownloadError = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_remote_storage_download_error",
			Help: "Number of errors when downloading remote archive files to local disk",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageDownloadSize = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_remote_storage_download_size",
			Help: "Total number of bytes that have been downloaded from remote storage to local disk",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageUploadCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_remote_storage_upload_count",
			Help: "Number of times a remote archive file has been uploaded from local disk",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageUploadError = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_remote_storage_upload_error",
			Help: "Number of errors when uploading remote archive files from local disk",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageUploadSize = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_remote_storage_upload_size",
			Help: "Total number of bytes that have been uploaded to remote storage from local disk",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageObjectsCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hoard_remote_storage_objects_count",
			Help: "Number of objects being stored remotely",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageObjectsSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hoard_remote_storage_objects_size",
			Help: "Total size of all objects being stored remotely",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
}

func RecordTaskCompletion(task string, feed *config.Feed, err error) {
	success := fmt.Sprintf("%t", err == nil)
	taskNumCompletions.WithLabelValues(task, feed.ID, success).Inc()
	taskLastCompletion.WithLabelValues(task, feed.ID, success).SetToCurrentTime()
}

func RecordSavedDownload(feed *config.Feed, size int) {
	downloadSavedCount.WithLabelValues(feed.ID).Inc()
	downloadSavedSize.WithLabelValues(feed.ID).Add(float64(size))
}

func RecordPack(feed *config.Feed, err error) {
	if err != nil {
		RecordPackFileErrors(feed, err)
		packFailedCount.WithLabelValues(feed.ID).Inc()
	} else {
		packCount.WithLabelValues(feed.ID).Inc()
	}
}

func RecordPackSizes(feed *config.Feed, unpacked int, packed int) {
	packUnpackedSize.WithLabelValues(feed.ID).Add(float64(unpacked))
	packPackedSize.WithLabelValues(feed.ID).Add(float64(packed))
}

func RecordPackFileErrors(feed *config.Feed, errs ...error) {
	packFileErrors.WithLabelValues(feed.ID).Add(float64(len(errs)))
}

func RecordAudit(feed *config.Feed, err error) {
	if err != nil {
		auditFailedCount.WithLabelValues(feed.ID).Inc()
	}
}

func RecordDiskUsage(subDir string, feedID string, count int, size int64) {
	localFilesCount.WithLabelValues(subDir, feedID).Set(float64(count))
	localFilesSize.WithLabelValues(subDir, feedID).Set(float64(size))
}

func RecordRemoteStorageDownload(storage *config.ObjectStorage, feed *config.Feed, err error, size int) {
	if err != nil {
		remoteStorageDownloadError.WithLabelValues(
			storage.Endpoint, storage.BucketName, storage.Prefix, feed.ID).Inc()
		return
	}
	remoteStorageDownloadCount.WithLabelValues(
		storage.Endpoint, storage.BucketName, storage.Prefix, feed.ID).Inc()
	remoteStorageDownloadSize.WithLabelValues(
		storage.Endpoint, storage.BucketName, storage.Prefix, feed.ID).Add(float64(size))
}

func RecordRemoteStorageUpload(storage *config.ObjectStorage, feed *config.Feed, err error, size int) {
	if err != nil {
		remoteStorageUploadError.WithLabelValues(
			storage.Endpoint, storage.BucketName, storage.Prefix, feed.ID).Inc()
		return
	}
	remoteStorageUploadCount.WithLabelValues(
		storage.Endpoint, storage.BucketName, storage.Prefix, feed.ID).Inc()
	remoteStorageUploadSize.WithLabelValues(
		storage.Endpoint, storage.BucketName, storage.Prefix, feed.ID).Add(float64(size))
}
func RecordRemoteStorageUsage(storage *config.ObjectStorage, feed *config.Feed, count int64, size int64) {
	remoteStorageObjectsCount.WithLabelValues(
		storage.Endpoint, storage.BucketName, storage.Prefix, feed.ID).Set(float64(count))
	remoteStorageObjectsSize.WithLabelValues(
		storage.Endpoint, storage.BucketName, storage.Prefix, feed.ID).Set(float64(size))
}
