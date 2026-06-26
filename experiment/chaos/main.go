package main

import (
	"context"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

const (
	defaultCrashMean   = 10.0
	defaultRestartMean = 60.0

	defaultCrashLambda   = 1 / defaultCrashMean
	defaultRestartLambda = 1 / defaultRestartMean

	defaultTaskManagerName = "taskmanager"
	defaultMode            = modeRunOnce

	envCrashLambda     = "LAMBDA_CRASH"
	envRestartLambda   = "LAMBDA_RESTART"
	envCrashMean       = "MEAN_CRASH_SECONDS"
	envRestartMean     = "MEAN_RESTART_SECONDS"
	envTaskManagerName = "TASKMANAGER_NAME"
	envMode            = "MODE"

	modeRunOnce    = "run_once"
	modeSimulation = "simulation"
)

// getOptionalEnvFloat64 reads a positive float64 from the environment.
// It returns false when the variable is missing or invalid.
func getOptionalEnvFloat64(key string) (float64, bool) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return 0, false
	}

	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed <= 0 {
		slog.Warn("Invalid environment variable, ignoring it",
			"env", key,
			"value", value)
		return 0, false
	}

	return parsed, true
}

// getEnvString reads a string value from the environment.
// If the variable is missing or empty, it returns the default value.
func getEnvString(key string, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}

	return value
}

// getExponentialLambda reads either a lambda value or a mean value.
// Lambda has priority over mean.
func getExponentialLambda(
	lambdaEnv string,
	meanEnv string,
	defaultLambda float64,
) float64 {
	if lambda, ok := getOptionalEnvFloat64(lambdaEnv); ok {
		return lambda
	}

	if mean, ok := getOptionalEnvFloat64(meanEnv); ok {
		return 1.0 / mean
	}

	return defaultLambda
}

// getMode reads and validates the execution mode.
func getMode() string {
	mode := strings.ToLower(getEnvString(envMode, defaultMode))

	switch mode {
	case modeRunOnce, modeSimulation:
		return mode
	default:
		slog.Warn("Invalid execution mode, using default value",
			"env", envMode,
			"value", mode,
			"default", defaultMode)
		return defaultMode
	}
}

// nextExponentialDelay calculates a delay using an exponential distribution.
func nextExponentialDelay(lambda float64) time.Duration {
	seconds := rand.ExpFloat64() / lambda
	return time.Duration(seconds * float64(time.Second))
}

// waitForDelay waits for the given delay or stops if the context is cancelled.
func waitForDelay(ctx context.Context, delay time.Duration) bool {
	select {
	case <-time.After(delay):
		return true
	case <-ctx.Done():
		return false
	}
}

// findTargetContainer returns a random running container matching the given name filter.
func findTargetContainer(
	ctx context.Context,
	cli *client.Client,
	containerNameFilter string,
) (*container.Summary, error) {
	result, err := cli.ContainerList(ctx, client.ContainerListOptions{})
	if err != nil {
		return nil, err
	}

	var matchingContainers []container.Summary

	for _, c := range result.Items {
		for _, name := range c.Names {
			normalizedName := strings.ToLower(name)

			if strings.Contains(normalizedName, containerNameFilter) {
				matchingContainers = append(matchingContainers, c)
				break
			}
		}
	}

	if len(matchingContainers) == 0 {
		return nil, nil
	}

	targetIndex := rand.Intn(len(matchingContainers))
	return &matchingContainers[targetIndex], nil
}

// getContainerDisplayName returns a readable container name.
func getContainerDisplayName(c container.Summary) string {
	if len(c.Names) > 0 {
		return c.Names[0]
	}

	return c.ID
}

// getShortContainerID returns the short Docker-like container ID.
func getShortContainerID(containerID string) string {
	if len(containerID) > 12 {
		return containerID[:12]
	}

	return containerID
}

// crashAndRestartOnce kills one matching container and restarts it after a delay.
func crashAndRestartOnce(
	ctx context.Context,
	cli *client.Client,
	containerNameFilter string,
	lambdaRestart float64,
) {
	target, err := findTargetContainer(ctx, cli, containerNameFilter)
	if err != nil {
		slog.Warn("Unable to list containers", "err", err)
		return
	}

	if target == nil {
		slog.Warn("No matching containers found",
			"ContainerNameFilter", containerNameFilter)
		return
	}

	targetName := getContainerDisplayName(*target)
	containerID := getShortContainerID(target.ID)

	slog.Info("Killing target container",
		"ContainerName", targetName,
		"ContainerID", containerID)

	_, err = cli.ContainerKill(ctx, target.ID, client.ContainerKillOptions{
		Signal: "SIGKILL",
	})
	if err != nil {
		slog.Error("Unable to kill target container",
			"ContainerName", targetName,
			"err", err)
		return
	}

	slog.Info("Container killed successfully",
		"ContainerName", targetName)

	restartDelay := nextExponentialDelay(lambdaRestart)
	slog.Info("Restart scheduled",
		"ContainerName", targetName,
		"Delay", restartDelay)

	if !waitForDelay(ctx, restartDelay) {
		slog.Info("Restart interrupted",
			"ContainerName", targetName)
		return
	}

	slog.Info("Restarting target container",
		"ContainerName", targetName,
		"ContainerID", containerID)

	_, err = cli.ContainerStart(ctx, target.ID, client.ContainerStartOptions{})
	if err != nil {
		slog.Error("Unable to restart target container",
			"ContainerName", targetName,
			"err", err)
		return
	}

	slog.Info("Container restarted successfully",
		"ContainerName", targetName)
}

// runOnceMode executes one immediate crash and one delayed restart.
func runOnceMode(
	ctx context.Context,
	cli *client.Client,
	containerNameFilter string,
	lambdaRestart float64,
) {
	slog.Info("Running in run_once mode")

	crashAndRestartOnce(ctx, cli, containerNameFilter, lambdaRestart)

	slog.Info("run_once mode completed")
}

// simulationMode repeatedly schedules crashes and restarts.
func simulationMode(
	ctx context.Context,
	cli *client.Client,
	containerNameFilter string,
	lambdaCrash float64,
	lambdaRestart float64,
) {
	slog.Info("Running in simulation mode")

	for {
		crashDelay := nextExponentialDelay(lambdaCrash)
		slog.Info("Next crash scheduled", "Delay", crashDelay)

		if !waitForDelay(ctx, crashDelay) {
			slog.Info("Chaos loop stopped")
			return
		}

		crashAndRestartOnce(ctx, cli, containerNameFilter, lambdaRestart)

		if ctx.Err() != nil {
			slog.Info("Chaos loop stopped")
			return
		}
	}
}

// main initializes the Docker client and starts the selected execution mode.
func main() {
	// Load configuration from environment variables.
	mode := getMode()

	lambdaCrash := getExponentialLambda(
		envCrashLambda,
		envCrashMean,
		defaultCrashLambda,
	)

	lambdaRestart := getExponentialLambda(
		envRestartLambda,
		envRestartMean,
		defaultRestartLambda,
	)

	taskManagerName := strings.ToLower(getEnvString(envTaskManagerName, defaultTaskManagerName))

	slog.Info("Starting Flink Chaos Simulator")
	slog.Info("-> Configuration", "Mode", mode)
	slog.Info("-> Configuration", "CrashLambda", lambdaCrash)
	slog.Info("-> Configuration", "RestartLambda", lambdaRestart)
	slog.Info("-> Configuration", "MeanCrashWaitSeconds", 1.0/lambdaCrash)
	slog.Info("-> Configuration", "MeanRestartWaitSeconds", 1.0/lambdaRestart)
	slog.Info("-> Configuration", "ContainerNameFilter", taskManagerName)

	// Create the Docker client using the environment configuration.
	cli, err := client.New(client.FromEnv)
	if err != nil {
		slog.Error("Unable to create Docker client", "err", err)
		os.Exit(1)
	}
	defer cli.Close()

	// Create a cancellable context for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for termination signals.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		slog.Info("Shutdown signal received")
		cancel()
	}()

	switch mode {
	case modeRunOnce:
		runOnceMode(ctx, cli, taskManagerName, lambdaRestart)

	case modeSimulation:
		simulationMode(ctx, cli, taskManagerName, lambdaCrash, lambdaRestart)
	}
}
