package main;

import "os"

func file_exists(filename string) bool {
    if _, err := os.Stat(filename); err == nil {
        return true
    }
    return false
}
