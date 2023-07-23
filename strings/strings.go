package strings

import (
	"fmt"
	"strconv"
	"time"
)

func Repeat(str string, n int) (out string) {
	for i := 0; i < n; i++ {
		out += str
	}
	return
}

func PadLeft(str string, length int, pad string) string {
	return Repeat(pad, length-len(str)) + str
}

func Generate(no int64, length int) string {
	return PadLeft(strconv.FormatInt(no, 10), length, "0")
}

func Format(prefix string, yyyy int, mm int, no int64, length int) string {
	return prefix + fmt.Sprintf(`%s_%s_%s`, Generate(int64(yyyy), 4), Generate(int64(mm), 2), Generate(no, length))
}

func GenerateCode(prefix string, no int64, length int) string {
	now := time.Now()
	return Format(prefix, now.Year(), int(now.Month()), no, length)
}
