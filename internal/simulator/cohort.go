package simulator

import (
	"math/rand"

	"github.com/mstone/fleetpulse/internal/ingest"
)

// Versions/OSNames/Regions are the pool of cohort values assigned randomly
// to each simulated client at startup. "2.6.0-beta" is included
// deliberately -- it's the version the --kill-cohort demo flag targets by
// default, standing in for a canary/beta rollout that goes dark.
var (
	Versions = []string{"2.4.0", "2.5.0", "2.5.1", "2.6.0-beta"}
	OSNames  = []string{"windows", "macos", "linux"}
	Regions  = []string{"us-east", "us-west", "eu", "apac"}
)

// RandomCohort picks a uniformly random cohort from the pools above. Real
// fleets are not uniformly distributed across versions (most clients sit on
// the last stable release, a small tail sits on a beta channel) -- see
// weightedVersion below, which NewClientPool actually uses.
func RandomCohort(rng *rand.Rand) ingest.Cohort {
	return ingest.Cohort{
		Version: weightedVersion(rng),
		OS:      OSNames[rng.Intn(len(OSNames))],
		Region:  Regions[rng.Intn(len(Regions))],
	}
}

// versionWeights loosely mimics a realistic adoption curve: most of the
// fleet on the newest stable release, a meaningful tail on the previous
// couple of releases, and a small beta cohort -- exactly the shape where a
// silent-cohort failure in the beta channel would be easy to miss without
// per-cohort signals (it's a small slice of aggregate traffic, but 100% of
// its own cohort).
var versionWeights = []struct {
	version string
	weight  int
}{
	{"2.5.1", 55},
	{"2.5.0", 25},
	{"2.4.0", 12},
	{"2.6.0-beta", 8},
}

func weightedVersion(rng *rand.Rand) string {
	total := 0
	for _, vw := range versionWeights {
		total += vw.weight
	}
	r := rng.Intn(total)
	for _, vw := range versionWeights {
		if r < vw.weight {
			return vw.version
		}
		r -= vw.weight
	}
	return versionWeights[0].version // unreachable in practice
}
