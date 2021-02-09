package monitoring

import (
	"github.com/jamespfennell/hoard/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var numDownloads *prometheus.CounterVec
var numFailedDownloads *prometheus.CounterVec
var numSavedDownloads *prometheus.CounterVec
var sizeSavedDownloads *prometheus.CounterVec

func init() {
	numDownloads = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_num_downloads",
			Help: "",
		},
		[]string{"feed_id"},
	)
	numFailedDownloads = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_num_failed_downloads",
			Help: "",
		},
		[]string{"feed_id"},
	)
	numSavedDownloads = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_num_saved_downloads",
			Help: "",
		},
		[]string{"feed_id"},
	)
	sizeSavedDownloads = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "hoard_size_saved_downloads",
			Help: "",
		},
		[]string{"feed_id"},
	)
}

func RecordSavedDownload(feed *config.Feed, size int) {
	numSavedDownloads.WithLabelValues(feed.ID).Inc()
	sizeSavedDownloads.WithLabelValues(feed.ID).Add(float64(size))
}

func RecordDownload(feed *config.Feed, err error) {
	if err != nil {
		numFailedDownloads.WithLabelValues(feed.ID).Inc()
	} else {
		numDownloads.WithLabelValues(feed.ID).Inc()
	}
}

/*
number of files downloaded successfully
number of file download errors
number of unique file downloads
size of the unique file downloads



number of archive operations
size of archives
size of bytes archived

*/
