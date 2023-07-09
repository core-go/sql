package strings

import (
	"fmt"
	"strconv"
	"time"
)

func times(str string, n int) (out string) {
	for i := 0; i < n; i++ {
		out += str
	}
	return
}

func PadLeft(str string, length int, pad string) string {
	return times(pad, length-len(str)) + str
}

func Generate(no int64, length int) string {
	return PadLeft(strconv.FormatInt(no, 10), length, "0")
}

func Format(yyyy int, mm int, no int64, length int) string {
	f := fmt.Sprintf(`%s_%s_%s`, Generate(int64(yyyy), 4), Generate(int64(mm), 2), Generate(no, length))
	return f
}

func GenerateCode(no int64, length int) string {
	now := time.Now()
	return Format(now.Year(), int(now.Month()), no, length)
}
