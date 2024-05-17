package utils

import "strings"

func StringPointer(val string) *string {
	return &val
}

func BoolPointer(val bool) *bool {
	return &val
}

func ParseTags(tags *string) map[string]string {
	tagsMap := make(map[string]string)
	if tags == nil || *tags == "" {
		println("No tags")
		return tagsMap
	}
	tagsList := strings.Split(*tags, ",")
	for _, tag := range tagsList {
		parts := strings.Split(tag, "=")
		if len(parts) == 2 && len(parts[0]) > 0 && len(parts[1]) > 0 {
			tagsMap[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return tagsMap
}
