package cli

import "fmt"

func humanizeBytes(bytesize int64) string {
	if bytesize < 0 {
		return fmt.Sprintf("-%s", humanizeBytes(-bytesize))
	}

	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	threshold := int64(1)

	for i, unit := range units {
		isFinal := i == len(units)-1
		nextThreshold := threshold * 1024
		if isFinal || bytesize < nextThreshold {
			if threshold == 1 {
				return fmt.Sprintf("%d %s", bytesize, unit)
			} else {
				amount := float64(bytesize) / float64(threshold)
				return fmt.Sprintf("%.2f %s", amount, unit)
			}
		}
		threshold = nextThreshold
	}

	panic("unreachable")
}
