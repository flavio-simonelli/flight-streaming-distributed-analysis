package config

import (
	"os"
	"strconv"
)

// Config holds the configuration settings for the flight simulator application.
// It includes parameters for input data, output type, Kafka settings, and performance tuning.
type Config struct {
	InputParquetPath          string  // Path to the input Parquet files containing flight data
	DataSourceType            string  // Type of datasource: "parquet" or "remote" (download/load TAR.GZ)
	InputArchivePath          string  // Path to the local compressed archive (tar.gz) for remote/cache datasource
	RemoteTarGzURL            string  // URL to download the remote dataset for "remote" data source
	RemoteTarGzSHA1           string  // Expected SHA1 hash of the downloaded remote dataset archive
	ExtractedCSVsDir          string  // Directory where TAR.GZ CSV files are extracted
	OutputType                string  // Type of output sink (e.g., "terminal", "kafka")
	KafkaBrokers              string  // Comma-separated list of Kafka broker addresses (e.g., "localhost:9092")
	KafkaTopic                string  // Kafka topic to which the flight records will be sent if using Kafka output
	MaxRecords                int     // Maximum number of records to process (0 means no limit)
	SpeedupFactor             int     // Factor to accelerate the timing of flight events (e.g., 100000 means 100,000 times faster than real time)
	SpinThresholdMs           int     // Active spin threshold in milliseconds for final precision (e.g., 2)
	OutOfOrderFactor          float64 // Probability [0.0, 1.0] that a record is published out of order to simulate multi-sensor latency
	OutOfOrderMaxDelayMinutes int     // Maximum out-of-order delay expressed in logical event-time minutes, scaled by SpeedupFactor at publish time
	ParquetReaderConcurrency  int64   // Number of parallel reader goroutines for loading Parquet data
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

	parquetConcurrency, _ := strconv.ParseInt(getEnv("PARQUET_READER_CONCURRENCY", "4"), 10, 64)
	if parquetConcurrency <= 0 {
		parquetConcurrency = 4
	}

	// Create and return a Config struct populated with values from environment variables or defaults.
	return &Config{
		InputParquetPath:          getEnv("INPUT_PARQUET_PATH", "./data/flights.parquet"),
		DataSourceType:            getEnv("DATA_SOURCE_TYPE", "parquet"),
		InputArchivePath:          getEnv("INPUT_ARCHIVE_PATH", "./data/project-1-data.tar.gz"),
		RemoteTarGzURL:            getEnv("REMOTE_TARGZ_URL", "http://www.ce.uniroma2.it/courses/sabd2526/project/project-1-data.tar.gz"),
		RemoteTarGzSHA1:           getEnv("REMOTE_TARGZ_SHA1", "17be276b72bd987e72598b0ea4907c3b19350606"),
		ExtractedCSVsDir:          getEnv("EXTRACTED_CSVS_DIR", "./data/extracted_csvs"),
		OutputType:                getEnv("OUTPUT_TYPE", "terminal"),
		KafkaBrokers:              getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:                getEnv("KAFKA_TOPIC", "flights-stream"),
		MaxRecords:                maxRecs,
		SpeedupFactor:             speedup,
		SpinThresholdMs:           spinMs,
		OutOfOrderFactor:          ooFactor,
		OutOfOrderMaxDelayMinutes: ooMaxDelayMinutes,
		ParquetReaderConcurrency:  parquetConcurrency,
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
