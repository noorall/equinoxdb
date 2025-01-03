package metric

import (
	"equinox/storage/config"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	labelNames = []string{"engine"}
)

func GetEngineLabs(opt config.Option) prometheus.Labels {
	if !opt.SeparateEnabled {
		return prometheus.Labels{
			"engine": "non-separate",
		}
	} else {
		return prometheus.Labels{
			"engine": "separate",
		}
	}
}
