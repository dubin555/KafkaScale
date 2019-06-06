package config

import (
	"github.com/spf13/viper"
	"strings"
	"github.com/lexkong/log"
)

type Config struct {
	Name string
}

func Init(cfg string) error {
	c := Config{
		Name:cfg,
	}

	if err := c.initConfig(); err != nil {
		return err
	}

	c.initLog()

	return nil
}

func (c *Config) initConfig() error {
	if c.Name == "dev" {
		//viper.SetConfigFile(c.Name)
		viper.AddConfigPath("conf")
		viper.SetConfigName("local")
	} else if c.Name == "lab" {
		viper.AddConfigPath("conf")
		viper.SetConfigName("lab")
	} else {
		viper.AddConfigPath("src/KafkaScale/conf")
		viper.SetConfigName("local")
	}
	viper.SetConfigType("yaml")
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)
	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	return nil
}

func (c *Config) initLog() {
	passLagerCfg := log.PassLagerCfg{
		Writers:        viper.GetString("log.writers"),
		LoggerLevel:    viper.GetString("log.logger_level"),
		LoggerFile:     viper.GetString("log.logger_file"),
		LogFormatText:  viper.GetBool("log.log_format_text"),
		RollingPolicy:  viper.GetString("log.rollingPolicy"),
		LogRotateDate:  viper.GetInt("log.log_rotate_date"),
		LogRotateSize:  viper.GetInt("log.log_rotate_size"),
		LogBackupCount: viper.GetInt("log.log_backup_count"),
	}

	log.InitWithConfig(&passLagerCfg)
}
