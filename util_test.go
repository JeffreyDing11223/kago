package kago

import (
	"fmt"
	"testing"
)

func TestListDir(t *testing.T) {
	fmt.Println(ListDir("./offsetCfg", "cfg"))
}
