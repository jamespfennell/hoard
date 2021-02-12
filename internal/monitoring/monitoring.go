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
	// TODO: label based on error type?
	packFileErrors.WithLabelValues(feed.ID).Add(float64(len(errs)))
}
