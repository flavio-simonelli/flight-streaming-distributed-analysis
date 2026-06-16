package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// Config holds the configuration settings for the flight simulator application.
// It includes parameters for input data, output type, Kafka settings, and performance tuning.
type Config struct {
	InputParquetPath          string  // Path to the input Parquet files containing flight data
	OutputType                string  // Type of output sink (e.g., "terminal", "kafka")
	KafkaBrokers              string  // Comma-separated list of Kafka broker addresses (e.g., "localhost:9092")
	KafkaTopic                string  // Kafka topic to which the flight records will be sent if using Kafka output
	MaxRecords                int     // Maximum number of records to process (0 means no limit)
	SpeedupFactor             int     // Factor to accelerate the timing of flight events (e.g., 100000 means 100,000 times faster than real time)
	SpinThresholdMs           int     // Active spin threshold in milliseconds for final precision (e.g., 2)
	OutOfOrderFactor          float64 // Probability [0.0, 1.0] that a record is published out of order to simulate multi-sensor latency
	OutOfOrderMaxDelayMinutes int     // Maximum out-of-order delay expressed in logical event-time minutes, scaled by SpeedupFactor at publish time
}

// LoadConfig reads configuration settings from environment variables and returns a Config struct.
// It provides default values for each setting if the corresponding environment variable is not set.
func LoadConfig() *Config {
	// Load environment variables from local .env file if it exists
	loadDotEnv(".env")

	maxRecs, _ := strconv.Atoi(getEnv("MAX_RECORDS", "0"))

	speedup, _ := strconv.Atoi(getEnv("SPEEDUP_FACTOR", "100000"))
	if speedup <= 0 {
		speedup = 1
	}

	spinMs, _ := strconv.Atoi(getEnv("SPIN_THRESHOLD_MS", "2"))
	if spinMs < 0 {
		spinMs = 0
	}

	// OutOfOrderFactor is clamped to [0.0, 1.0]
	ooFactor, _ := strconv.ParseFloat(getEnv("OUT_OF_ORDER_FACTOR", "0.0"), 64)
	if ooFactor < 0.0 {
		ooFactor = 0.0
	} else if ooFactor > 1.0 {
		ooFactor = 1.0
	}

	ooMaxDelayMinutes, _ := strconv.Atoi(getEnv("OUT_OF_ORDER_MAX_DELAY_MINUTES", "5"))
	if ooMaxDelayMinutes < 0 {
		ooMaxDelayMinutes = 0
	}

	// Create and return a Config struct populated with values from environment variables or defaults.
	return &Config{
		InputParquetPath:          getEnv("INPUT_PARQUET_PATH", "./data/flights.parquets"),
		OutputType:                getEnv("OUTPUT_TYPE", "terminal"),
		KafkaBrokers:              getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:                getEnv("KAFKA_TOPIC", "flights-stream"),
		MaxRecords:                maxRecs,
		SpeedupFactor:             speedup,
		SpinThresholdMs:           spinMs,
		OutOfOrderFactor:          ooFactor,
		OutOfOrderMaxDelayMinutes: ooMaxDelayMinutes,
	}
}

// getEnv is a helper function that retrieves the value of an environment variable.
// If the variable is not set, it returns a specified fallback value.
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// loadDotEnv parses a local .env file and sets environment variables if they are not already set.
func loadDotEnv(filepath string) {
	file, err := os.Open(filepath)
	if err != nil {
		return // Ignore if file does not exist
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Strip quotes if they wrap the value
		if (strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) ||
			(strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) {
			if len(value) >= 2 {
				value = value[1 : len(value)-1]
			}
		}

		// Only set env variable if it is not already defined in the OS environment
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, value)
		}
	}
}
