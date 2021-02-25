package monitoring

import (
	"github.com/jamespfennell/hoard/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

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
	downloadCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_download_count",
			Help: "",
		},
		[]string{"feed_id"},
	)
	downloadFailedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_download_failed_count",
			Help: "",
		},
		[]string{"feed_id"},
	)
	downloadSavedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_download_saved_count",
			Help: "",
		},
		[]string{"feed_id"},
	)
	downloadSavedSize = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_download_saved_size",
			Help: "",
		},
		[]string{"feed_id"},
	)
	packCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_pack_count",
			Help: "",
		},
		[]string{"feed_id"},
	)
	packFailedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_pack_failed_count",
			Help: "",
		},
		[]string{"feed_id"},
	)
	packUnpackedSize = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_pack_unpacked_size",
			Help: "",
		},
		[]string{"feed_id"},
	)
	packPackedSize = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_pack_packed_size",
			Help: "",
		},
		[]string{"feed_id"},
	)
	packFileErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_pack_file_errors",
			Help: "",
		},
		[]string{"feed_id"},
	)
	uploadCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_upload_count",
			Help: "",
		},
		[]string{"feed_id"},
	)
	uploadFailedCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_upload_failed_count",
			Help: "",
		},
		[]string{"feed_id"},
	)
	localFilesCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hoard_local_files_count",
			Help: "",
		},
		[]string{"directory", "feed_id"},
	)
	localFilesSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hoard_local_files_size",
			Help: "",
		},
		[]string{"directory", "feed_id"},
	)
	remoteStorageDownloadCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_remote_storage_download_count",
			Help: "",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageDownloadError = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_remote_storage_download_error",
			Help: "",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageDownloadSize = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_remote_storage_download_size",
			Help: "",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageUploadCount = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_remote_storage_upload_count",
			Help: "",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageUploadError = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_remote_storage_upload_error",
			Help: "",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageUploadSize = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_remote_storage_upload_size",
			Help: "",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageObjectsCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hoard_remote_storage_objects_count",
			Help: "",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
	remoteStorageObjectsSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "hoard_remote_storage_objects_size",
			Help: "",
		},
		[]string{"endpoint", "bucket", "prefix", "feed_id"},
	)
}

func RecordSavedDownload(feed *config.Feed, size int) {
	downloadSavedCount.WithLabelValues(feed.ID).Inc()
	downloadSavedSize.WithLabelValues(feed.ID).Add(float64(size))
}

func RecordDownload(feed *config.Feed, err error) {
	if err != nil {
		downloadFailedCount.WithLabelValues(feed.ID).Inc()
	} else {
		downloadCount.WithLabelValues(feed.ID).Inc()
	}
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

func RecordUpload(feed *config.Feed, err error) {
	if err != nil {
		uploadFailedCount.WithLabelValues(feed.ID).Inc()
	} else {
		uploadCount.WithLabelValues(feed.ID).Inc()
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
