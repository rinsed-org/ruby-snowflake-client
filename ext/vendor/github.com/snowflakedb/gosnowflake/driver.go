// Copyright (c) 2017-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"sync"
)

var paramsMutex *sync.Mutex

// SnowflakeDriver is a context of Go Driver
type SnowflakeDriver struct{}

// Open creates a new connection.
func (d SnowflakeDriver) Open(dsn string) (driver.Conn, error) {
	logger.Info("Open")
	ctx := context.TODO()
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return d.OpenWithConfig(ctx, *cfg)
}

// OpenWithConfig creates a new connection with the given Config.
func (d SnowflakeDriver) OpenWithConfig(
	ctx context.Context,
	config Config) (
	driver.Conn, error) {
	if config.Tracing != "" {
		logger.SetLogLevel(config.Tracing)
	}
	fmt.Println("in #open_with_config")
	logger.Info("OpenWithConfig")
	sc, err := buildSnowflakeConn(ctx, config)
	if err != nil {
		return nil, err
	}

	if err = authenticateWithConfig(sc); err != nil {
		return nil, err
	}
	fmt.Println("in #open_with_config past auth")
	//sc.connectionTelemetry(&config)

	fmt.Println("in #open_with_config later")
	sc.startHeartBeat()
	fmt.Println("in #after heartbeat")
	sc.internal = &httpClient{sr: sc.rest}
	return sc, nil
}

func runningOnGithubAction() bool {
	return os.Getenv("GITHUB_ACTIONS") != ""
}

var logger = CreateDefaultLogger()

func init() {
	sql.Register("snowflake", &SnowflakeDriver{})
	logger.SetLogLevel("error")
	if runningOnGithubAction() {
		logger.SetLogLevel("fatal")
	}
	paramsMutex = &sync.Mutex{}
}
