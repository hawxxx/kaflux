package model

type ACL struct {
	ResourceType string  `json:"resourceType"`
	ResourceName string  `json:"resourceName"`
	PatternType  string  `json:"patternType"`
	Principal    string  `json:"principal"`
	Host         string  `json:"host"`
	Operation    string  `json:"operation"`
	Permission   string  `json:"permission"`
	Raw          *ACLRaw `json:"raw,omitempty"`
}
type ACLRaw struct {
	ResourceType int8 `json:"resourceType"`
	PatternType  int8 `json:"patternType"`
	Operation    int8 `json:"operation"`
	Permission   int8 `json:"permission"`
}
