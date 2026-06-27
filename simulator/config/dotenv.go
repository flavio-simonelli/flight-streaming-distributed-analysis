package config

import (
	"bufio"
	"os"
	"strings"
)

// loadDotEnv loads environment variables from a .env file when it exists.
func loadDotEnv(filepath string) {
	file, err := os.Open(filepath)
	if err != nil {
		return // Missing .env files are ignored.
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Strip matching quotes around the value, if any.
		if (strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) ||
			(strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) {
			if len(value) >= 2 {
				value = value[1 : len(value)-1]
			}
		}

		// Do not override variables that are already defined in the OS environment.
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, value)
		}
	}

	_ = scanner.Err()
}
