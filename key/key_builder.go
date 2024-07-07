package key

type KeyBuilder interface {
	BuildKey(object interface{}) string
	BuildKeyFromMap(keyMap map[string]interface{}, idNames []string) string
}
