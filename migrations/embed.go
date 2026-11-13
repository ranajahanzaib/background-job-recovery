package migrations

import "embed"

// FS contains the versioned schema migrations shipped with the application.
//
//go:embed *.sql
var FS embed.FS
