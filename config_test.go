package main

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("写入临时配置失败: %v", err)
	}
	return path
}

func TestLoadConfigDefaults(t *testing.T) {
	cases := []struct {
		name   string
		yaml   string
		assert func(t *testing.T, c *Config)
	}{
		{
			name: "空文件全部取默认",
			yaml: "",
			assert: func(t *testing.T, c *Config) {
				if c.ListenAddr != ":8080" {
					t.Errorf("ListenAddr = %q, want :8080", c.ListenAddr)
				}
				if c.StorageMode != "discard" {
					t.Errorf("StorageMode = %q, want discard", c.StorageMode)
				}
				if c.MemoryDir != "/tmp/dl_cache" || c.DiskDir != "/data/downloads" {
					t.Errorf("目录默认值不符: memory=%q disk=%q", c.MemoryDir, c.DiskDir)
				}
				if c.MaxRateMbps != 0 || c.DailyLimitGB != 0 {
					t.Errorf("限速/配额默认应为 0: rate=%d daily=%v", c.MaxRateMbps, c.DailyLimitGB)
				}
				if c.MaxConnections != 2 {
					t.Errorf("MaxConnections = %d, want 2", c.MaxConnections)
				}
				if c.Schedule != (ScheduleConfig{StartHour: 0, EndHour: 24}) {
					t.Errorf("Schedule = %+v, want {0 24}", c.Schedule)
				}
				if c.Jitter != (JitterConfig{MinSeconds: 5, MaxSeconds: 30}) {
					t.Errorf("Jitter = %+v, want {5 30}", c.Jitter)
				}
				if c.CooldownHours != 6 {
					t.Errorf("CooldownHours = %d, want 6", c.CooldownHours)
				}
				if !strings.Contains(c.UserAgent, "Chrome/") {
					t.Errorf("UserAgent 默认值异常: %q", c.UserAgent)
				}
				if c.Targets == nil || len(c.Targets) != 0 {
					t.Errorf("Targets 应为空切片，实际 %#v", c.Targets)
				}
			},
		},
		{
			name: "部分字段只覆盖自身",
			yaml: "listen_addr: \":9090\"\nmax_connections: 5\n",
			assert: func(t *testing.T, c *Config) {
				if c.ListenAddr != ":9090" || c.MaxConnections != 5 {
					t.Errorf("显式字段未生效: addr=%q conn=%d", c.ListenAddr, c.MaxConnections)
				}
				if c.StorageMode != "discard" || c.Schedule != (ScheduleConfig{0, 24}) {
					t.Errorf("其余字段应保持默认: mode=%q sched=%+v", c.StorageMode, c.Schedule)
				}
			},
		},
		{
			name: "schedule 块只给 start 时 end 取默认 24",
			yaml: "schedule:\n  start_hour: 1\n",
			assert: func(t *testing.T, c *Config) {
				if c.Schedule != (ScheduleConfig{StartHour: 1, EndHour: 24}) {
					t.Errorf("Schedule = %+v, want {1 24}", c.Schedule)
				}
			},
		},
		{
			name: "跨夜 schedule 原样保留",
			yaml: "schedule:\n  start_hour: 23\n  end_hour: 7\n",
			assert: func(t *testing.T, c *Config) {
				if c.Schedule != (ScheduleConfig{StartHour: 23, EndHour: 7}) {
					t.Errorf("Schedule = %+v, want {23 7}", c.Schedule)
				}
			},
		},
		{
			name: "target 缺省 weight/enabled 取默认",
			yaml: "targets:\n  - name: demo\n    url: https://example.com/a.bin\n",
			assert: func(t *testing.T, c *Config) {
				if len(c.Targets) != 1 {
					t.Fatalf("Targets 数量 = %d, want 1", len(c.Targets))
				}
				got := c.Targets[0]
				if got.Weight != 10 || !got.Enabled {
					t.Errorf("target 默认值不符: %+v", got)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadConfig(writeTempConfig(t, tc.yaml))
			if err != nil {
				t.Fatalf("LoadConfig 意外失败: %v", err)
			}
			tc.assert(t, cfg)
		})
	}
}

func TestLoadConfigPresence(t *testing.T) {
	const content = `
max_rate_mbps: 0
daily_limit_gb: 0
cooldown_hours: 1
jitter_seconds:
  min_seconds: 0
  max_seconds: 0
targets:
  - name: 显式最小权重
    url: https://example.com/a.bin
    weight: 1
    enabled: false
`
	cfg, err := LoadConfig(writeTempConfig(t, content))
	if err != nil {
		t.Fatalf("LoadConfig 意外失败: %v", err)
	}
	if cfg.MaxRateMbps != 0 || cfg.DailyLimitGB != 0 {
		t.Errorf("显式 0 未被保留: rate=%d daily=%v", cfg.MaxRateMbps, cfg.DailyLimitGB)
	}
	if cfg.CooldownHours != 1 {
		t.Errorf("CooldownHours = %d, want 1", cfg.CooldownHours)
	}
	if cfg.Jitter != (JitterConfig{MinSeconds: 0, MaxSeconds: 0}) {
		t.Errorf("显式 jitter 0 未被保留: %+v", cfg.Jitter)
	}
	if len(cfg.Targets) != 1 || cfg.Targets[0].Weight != 1 || cfg.Targets[0].Enabled {
		t.Errorf("显式 weight=1/enabled=false 未被保留: %+v", cfg.Targets)
	}
}

func TestLoadConfigInvalid(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{"storage_mode 非法", "storage_mode: ram\n"},
		{"storage_mode 空串", "storage_mode: \"\"\n"},
		{"listen_addr 缺端口", "listen_addr: \"8080\"\n"},
		{"listen_addr 端口越界", "listen_addr: \":70000\"\n"},
		{"max_rate_mbps 为负", "max_rate_mbps: -1\n"},
		{"max_connections 为 0", "max_connections: 0\n"},
		{"max_connections 超过 10", "max_connections: 11\n"},
		{"daily_limit_gb 为负", "daily_limit_gb: -0.5\n"},
		{"daily_limit_gb 为 NaN", "daily_limit_gb: .nan\n"},
		{"schedule start 越界", "schedule:\n  start_hour: 24\n  end_hour: 24\n"},
		{"schedule end 越界", "schedule:\n  start_hour: 0\n  end_hour: 25\n"},
		{"schedule start=end", "schedule:\n  start_hour: 5\n  end_hour: 5\n"},
		{"schedule start 为负", "schedule:\n  start_hour: -1\n  end_hour: 10\n"},
		{"jitter min>max", "jitter_seconds:\n  min_seconds: 10\n  max_seconds: 5\n"},
		{"jitter 为负", "jitter_seconds:\n  min_seconds: -1\n"},
		{"cooldown_hours 为 0", "cooldown_hours: 0\n"},
		{"cooldown_hours 为负", "cooldown_hours: -3\n"},
		{"user_agent 空串", "user_agent: \"\"\n"},
		{"target name 为空", "targets:\n  - url: https://example.com/a.bin\n    weight: 1\n"},
		{"target url 非 http", "targets:\n  - name: a\n    url: ftp://example.com/a.bin\n    weight: 1\n"},
		{"target url 无 host", "targets:\n  - name: a\n    url: https://\n    weight: 1\n"},
		{"target url 含 userinfo", "targets:\n  - name: a\n    url: https://u:p@example.com/a.bin\n    weight: 1\n"},
		{"target weight 为 0", "targets:\n  - name: a\n    url: https://example.com/a.bin\n    weight: 0\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadConfig(writeTempConfig(t, tc.yaml)); err == nil {
				t.Fatalf("应报错但成功: %s", tc.yaml)
			}
		})
	}
}

func TestLoadConfigMissingFile(t *testing.T) {
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("配置文件不存在时应报错")
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		mut  func(c *Config)
	}{
		{
			name: "默认配置",
			mut:  func(c *Config) {},
		},
		{
			name: "显式零值与跨夜窗口",
			mut: func(c *Config) {
				c.MaxRateMbps = 0
				c.DailyLimitGB = 0
				c.Jitter = JitterConfig{MinSeconds: 0, MaxSeconds: 0}
				c.Schedule = ScheduleConfig{StartHour: 23, EndHour: 7}
				c.StorageMode = "memory"
				c.Targets = []TargetConfig{
					{Name: "禁用目标", URL: "https://example.com/a.bin", Weight: 1, Enabled: false},
					{Name: "启用目标", URL: "http://example.org/b.iso", Weight: 99, Enabled: true},
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mut(cfg)

			path := filepath.Join(t.TempDir(), "sub", "config.yaml")
			if err := cfg.Save(path); err != nil {
				t.Fatalf("Save 失败: %v", err)
			}
			if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
				t.Errorf("临时文件应已清理，stat err=%v", err)
			}
			loaded, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("LoadConfig 失败: %v", err)
			}
			if !reflect.DeepEqual(cfg, loaded) {
				t.Errorf("往返不一致:\n原值: %+v\n加载: %+v", cfg, loaded)
			}
		})
	}
}

func TestSaveRejectsInvalid(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxConnections = 99
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := cfg.Save(path); err == nil {
		t.Fatal("非法配置保存应报错")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("非法配置不应落盘，stat err=%v", err)
	}
}

func TestExampleConfigLoads(t *testing.T) {
	cfg, err := LoadConfig("config.example.yaml")
	if err != nil {
		t.Fatalf("示例配置应可加载: %v", err)
	}
	if len(cfg.Targets) < 12 {
		t.Fatalf("示例目标数 = %d, want >= 12", len(cfg.Targets))
	}
	for i, tg := range cfg.Targets {
		if tg.Name == "" {
			t.Errorf("targets[%d] 缺少 name", i)
		}
	}
}

func TestValidateHelpers(t *testing.T) {
	if err := ValidateMaxRateMbps(0); err != nil {
		t.Errorf("0 应合法: %v", err)
	}
	if err := ValidateMaxRateMbps(-1); err == nil {
		t.Error("-1 应非法")
	}
	for _, v := range []int{1, 2, 10} {
		if err := ValidateMaxConnections(v); err != nil {
			t.Errorf("%d 应合法: %v", v, err)
		}
	}
	for _, v := range []int{0, 11} {
		if err := ValidateMaxConnections(v); err == nil {
			t.Errorf("%d 应非法", v)
		}
	}
	if err := ValidateDailyLimitGB(math.Inf(1)); err == nil {
		t.Error("Inf 应非法")
	}
	if err := ValidateSchedule(ScheduleConfig{StartHour: 23, EndHour: 7}); err != nil {
		t.Errorf("跨夜窗口应合法: %v", err)
	}
	if err := ValidateJitter(JitterConfig{MinSeconds: 0, MaxSeconds: 0}); err != nil {
		t.Errorf("jitter 0/0 应合法: %v", err)
	}
	if err := ValidateJitter(JitterConfig{MinSeconds: 10, MaxSeconds: 5}); err == nil {
		t.Error("jitter min>max 应非法")
	}
	if err := ValidateTargetInput("name", "HTTPS://Example.com/A", 1); err != nil {
		t.Errorf("大小写 scheme 应合法: %v", err)
	}
}
