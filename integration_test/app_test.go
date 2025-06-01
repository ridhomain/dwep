package integration_test

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupTestApp creates a new instance of the main application service for testing
func setupTestApp(ctx context.Context, networkName string, env *TestEnvironment, envOverrides map[string]string) (testcontainers.Container, string, error) {
	// Get the absolute path to the project root
	projectRoot, err := getProjectRoot()
	if err != nil {
		return nil, "", fmt.Errorf("failed to get project root: %w", err)
	}

	// Base environment variables
	baseEnv := map[string]string{
		"POSTGRES_DSN": env.PostgresDSNNetwork,
		"NATS_URL":     env.NATSURLNetwork,
		"COMPANY_ID":   env.CompanyID,
		"LOG_LEVEL":    "debug",
		"SERVER_PORT":  "8080",
		"ENVIRONMENT":  "test",
	}

	// Merge overrides into base environment
	// Overrides will take precedence
	if envOverrides != nil {
		for k, v := range envOverrides {
			baseEnv[k] = v
		}
	}

	// Application container configuration
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    projectRoot,
			Dockerfile: "Dockerfile",
		},
		Networks: []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {"message-service"},
		},
		ExposedPorts: []string{"8080/tcp"},
		Env:          baseEnv, // Use the merged environment
		WaitingFor:   wait.ForLog("Both consumers started successfully"),
	}

	// Start the application container
	app, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
		Reuse:            false,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to start application container: %w", err)
	}

	// Get the application API address
	host, err := app.Host(ctx)
	if err != nil {
		return app, "", fmt.Errorf("failed to get application host: %w", err)
	}

	mappedPort, err := app.MappedPort(ctx, "8080")
	if err != nil {
		return app, "", fmt.Errorf("failed to get application mapped port: %w", err)
	}

	// Format application API address
	apiAddress := fmt.Sprintf("http://%s:%s", host, mappedPort.Port())

	return app, apiAddress, nil
}
