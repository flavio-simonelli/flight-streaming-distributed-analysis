package config

import (
	"bufio"
	"os"
	"strings"
)

// loadDotEnv loads environment variables from a .env file.
func loadDotEnv(filepath string) {
	file, err := os.Open(filepath)
	if err != nil {
		return // File missing is a no-op
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

		// Remove enclosing quotes if present
		if (strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) ||
			(strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) {
			if len(value) >= 2 {
				value = value[1 : len(value)-1]
			}
		}

		// Keep existing OS environment variable if already set
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, value)
		}
	}
}
