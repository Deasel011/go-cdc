// Package cdc is the vocabulary for change data capture.
//
// A Capture produces committed Changes. A Data store applies them.
// Engine-specific SQL lives in data, and JetStream transport lives in
// stream/jetstream. This package imports neither.
package cdc
