package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	formatCmd = "indent"
)

var (
	ostype = runtime.GOOS
)

func formatCode(path string, info os.FileInfo, err error) error {
	curpath, _ := os.Getwd()
	curpath += "/"
	if info == nil {
		fmt.Println("can't find ", path)
		goto _FAILED
	}
	if info.IsDir() {
		goto _FAILED
	}
	curpath += path
	if strings.HasSuffix(curpath, ".c") || strings.HasSuffix(curpath, ".h") || strings.HasSuffix(curpath, ".cc") {
		formatCmd := exec.Command(formatCmd, "-npro", "-gnu", "-i4", "-ts4", "-sob", "-l200", "-ss", "-bl", "-bli", "0", "-npsl", curpath)
		if err := formatCmd.Run(); err != nil {
			goto _FAILED
		}
	}
_FAILED:
	return nil
}
func delBakupFile(path string, info os.FileInfo, err error) error {
	var curpath string
	curpath, _ = os.Getwd()
	curpath += "/"
	if info == nil {
		fmt.Println("can't find ", path)
		goto _FAILED
	}
	if info.IsDir() {
		goto _FAILED
	}
	curpath += path
	if strings.HasSuffix(curpath, ".c~") || strings.HasSuffix(curpath, ".h~") || strings.HasSuffix(curpath, ".cc~") {
		if os.Remove(curpath) != nil {
			goto _FAILED
		}
	}
_FAILED:
	return nil
}
func execFormatAction(root string) {
	err := filepath.Walk(root, formatCode)
	if err != nil {
		fmt.Println(err)
	}
	err = filepath.Walk(root, delBakupFile)
	if err != nil {
		fmt.Println(err)
	}
}
func main() {
	if ostype != "linux" {
		fmt.Println("just using in linux system")
	} else {
		_, err := exec.LookPath("indent")
		if err != nil {
			fmt.Println("indent not be installed")
		} else {
			execFormatAction("./")
		}

	}
}
