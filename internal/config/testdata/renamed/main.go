// This is a fixture, not a build target: everything under testdata/ is invisible to the go tool, so this
// file is only ever read by the section guard's own parser.
//
// It is the case the guard used to miss. The service holds its configuration in a variable named
// something other than "cfg" — legal Go, and nothing in the repository forbids it — reads two sections
// off it, and declares only one of them. A guard that recognised reads by the literal name "cfg" saw no
// read at all here and stayed silent.
package main

import "github.com/martialanouman/go-gateway/internal/config"

// Each of the three declaration forms the guard derives from reads a DIFFERENT section, so no branch of
// configHolders can be removed without the assertion noticing. A fixture where one form reads nothing
// would let its branch rot unnoticed.
func main() {
	// Form 1: the result of config.Load.
	conf, err := config.Load("renamed-svc", config.SectionPostgres)
	if err != nil {
		panic(err)
	}
	_ = conf.Kafka.Brokers

	// Form 2: an explicitly declared variable.
	var settings config.Config
	_ = settings.Redis.URL

	open(conf)
}

// Form 3: a function parameter — the wiring-style signature the real services use.
func open(other config.Config) {
	_ = other.Postgres.MaxConns
}
