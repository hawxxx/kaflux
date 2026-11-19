package mcp

// Dispatcher runs tool calls against the in-process REST handler. The
// implementation lands with the dispatcher; the type exists now so Tool.Preview
// has a stable signature.
type Dispatcher struct{}
