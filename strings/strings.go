package strings

import "strings"

func IsQuoted(s string) bool {
	return len(s) > 2 &&
		((strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) ||
			(strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`)))
}

func TrimQuotes(s string) string {
	if IsQuoted(s) {
		return s[1 : len(s)-1]
	}

	return s
}
