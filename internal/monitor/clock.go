package monitor

import "time"

// nowMillis 返回墙钟的毫秒 epoch 值。
func nowMillis() int64 { return time.Now().UnixMilli() }
