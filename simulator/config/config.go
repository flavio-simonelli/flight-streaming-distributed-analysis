package config

import (
	"os"
	"strconv"
)

// Config defines the application settings loaded from the environment.
type Config struct {
	InputArchivePath          string  // Path to the local dataset compressed archive (.tar.gz)
	RemoteTarGzURL            string  // URL of the remote dataset archive
	TargzSHA1                 string  // Expected SHA1 hash of the remote dataset archive
	OutputType                string  // Type of sink to output results ("terminal", "kafka")
	KafkaBrokers              string  // Comma-separated list of Kafka broker addresses
	KafkaTopic                string  // Destination Kafka topic
	MaxRecords                int     // Limit on records to process (0 means no limit)
	SpeedupFactor             int     // Factor to accelerate event emission rate
	SpinThresholdMs           int     // Active spin wait threshold in milliseconds
	OutOfOrderFactor          float64 // Probability [0.0, 1.0] of injecting out-of-order latency
	OutOfOrderMaxDelayMinutes int     // Maximum delay in minutes for out-of-order injection
}

// LoadConfig initializes Config from environment variables and sets defaults.
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
		InputArchivePath:          getEnv("INPUT_ARCHIVE_PATH", "./data/project-1-data.tar.gz"),
		RemoteTarGzURL:            getEnv("REMOTE_TARGZ_URL", "http://www.ce.uniroma2.it/courses/sabd2526/project/project-1-data.tar.gz"),
		TargzSHA1:                 getEnv("TARGZ_SHA1", "17be276b72bd987e72598b0ea4907c3b19350606"),
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

// getEnv retrieves the value of an environment variable or returns the fallback.
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
