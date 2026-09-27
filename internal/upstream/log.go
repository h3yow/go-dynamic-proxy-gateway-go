package upstream

import "log"

var logger = log.Default()

func logf(format string, args ...any) { logger.Printf("[upstream] "+format, args...) }
