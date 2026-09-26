package engine

import "time"

func monitorNow() int64 { return time.Now().UnixMilli() }
