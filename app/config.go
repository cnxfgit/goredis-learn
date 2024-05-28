package app

import "os"

type Config struct {
	Bind                    string `cfg:"bind"`
	Port                    int    `cfg:port`
	AppendOnly_             bool   `cfg:appendonly`
	AppendFileName_         string `cfg:appendfilename`
	AppendFsync_            string `cfg:appendfsync`
	AutoAofRewriteAfterCmd_ int    `cfg:auto-aof-rewrite-after-cmds`
}

func SetUpConfig() *Config {
	confOnce.Do(func() {
		defer func() {
			if globalConf == nil {
				globalConf = defaultConf()
			}
		}()

		file, err := os.Open("./redis.conf")
		globalConf = SetUpConfig(file)
	})

	return globalConf
}

func defaultConf() *Config {
	return &Config{
		Bind:        "0.0.0.0",
		Port:        6379,
		AppendOnly_: false,
	}
}
