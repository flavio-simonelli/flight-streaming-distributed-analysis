package config

import (
	"os"
	"strconv"
)

// Config holds the configuration settings for the flight simulator application.
// It includes parameters for input data, output type, Kafka settings, and performance tuning.
type Config struct {
	InputParquetPath string // Path to the input Parquet files containing flight data
	OutputType       string // Type of output sink (e.g., "terminal", "kafka")
	KafkaBrokers     string // Comma-separated list of Kafka broker addresses (e.g., "localhost:9092")
	KafkaTopic       string // Kafka topic to which the flight records will be sent if using Kafka output
	MaxRecords       int    // Maximum number of records to process (0 means no limit)
	SpeedupFactor    int    // Factor to accelerate the timing of flight events (e.g., 100000 means 100,000 times faster than real time)
}

// LoadConfig reads configuration settings from environment variables and returns a Config struct.
// It provides default values for each setting if the corresponding environment variable is not set.
func LoadConfig() *Config {

	// Convert MaxRecords and SpeedupFactor from string to int, using default values if not set.
	maxRecs, _ := strconv.Atoi(getEnv("MAX_RECORDS", "0"))
	speedup, _ := strconv.Atoi(getEnv("SPEEDUP_FACTOR", "100000")) // Default: accelera di 100.000 volte

	// Create and return a Config struct populated with values from environment variables or defaults.
	return &Config{
		InputParquetPath: getEnv("INPUT_PARQUET_PATH", "./data/flights.parquets"),
		OutputType:       getEnv("OUTPUT_TYPE", "terminal"),
		KafkaBrokers:     getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:       getEnv("KAFKA_TOPIC", "flights-stream"),
		MaxRecords:       maxRecs,
		SpeedupFactor:    speedup,
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
