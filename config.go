package main

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// 配置默认值与枚举常量。
const (
	defaultListenAddr     = ":8080"
	defaultStorageMode    = "discard"
	defaultMemoryDir      = "/tmp/dl_cache"
	defaultDiskDir        = "/data/downloads"
	defaultMaxConnections = 2
	defaultJitterMin      = 5
	defaultJitterMax      = 30
	defaultCooldownHours  = 6
	defaultTargetWeight   = 10
	defaultUserAgent      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

	storageModeDiscard = "discard"
	storageModeMemory  = "memory"
	storageModeDisk    = "disk"
)

// ScheduleConfig 每日活跃时间段 [StartHour, EndHour)。
// EndHour=24 表示到当日午夜；EndHour < StartHour 表示跨夜窗口（如 23→7）。
type ScheduleConfig struct {
	StartHour int `yaml:"start_hour" json:"start_hour"` // 0-23
	EndHour   int `yaml:"end_hour"   json:"end_hour"`   // 0-24，仅允许作为结束时刻
}

// JitterConfig 单个文件下载完成后的随机休眠区间（秒）。
type JitterConfig struct {
	MinSeconds int `yaml:"min_seconds" json:"min_seconds"` // 默认 5，>=0
	MaxSeconds int `yaml:"max_seconds" json:"max_seconds"` // 默认 30，>=0 且 >= MinSeconds
}

// TargetConfig 一条下载目标配置。
type TargetConfig struct {
	Name    string `yaml:"name" json:"name"`
	URL     string `yaml:"url"  json:"url"`
	Weight  int    `yaml:"weight" json:"weight"`   // 默认 10，>=1
	Enabled bool   `yaml:"enabled" json:"enabled"` // 默认 true
}

// Config 是 NetSuck 的完整运行配置。
type Config struct {
	ListenAddr     string         `yaml:"listen_addr"`     // 默认 ":8080"
	StorageMode    string         `yaml:"storage_mode"`    // discard|memory|disk，默认 discard
	MemoryDir      string         `yaml:"memory_dir"`      // 默认 "/tmp/dl_cache"
	DiskDir        string         `yaml:"disk_dir"`        // 默认 "/data/downloads"
	MaxRateMbps    int            `yaml:"max_rate_mbps"`   // 默认 0=不限速，>=0
	MaxConnections int            `yaml:"max_connections"` // 默认 2，范围 1-10
	DailyLimitGB   float64        `yaml:"daily_limit_gb"`  // 默认 0=不限量，有限非负数
	Schedule       ScheduleConfig `yaml:"schedule"`        // 默认 {0,24}=全天
	Jitter         JitterConfig   `yaml:"jitter_seconds"`
	CooldownHours  int            `yaml:"cooldown_hours"` // 默认 6，>0
	UserAgent      string         `yaml:"user_agent"`     // 默认桌面 Chrome UA
	Targets        []TargetConfig `yaml:"targets"`
}

// 用于 presence（字段是否出现）检测的原始结构：指针字段为 nil 表示 YAML 中缺该字段。
type rawConfig struct {
	ListenAddr     *string      `yaml:"listen_addr"`
	StorageMode    *string      `yaml:"storage_mode"`
	MemoryDir      *string      `yaml:"memory_dir"`
	DiskDir        *string      `yaml:"disk_dir"`
	MaxRateMbps    *int         `yaml:"max_rate_mbps"`
	MaxConnections *int         `yaml:"max_connections"`
	DailyLimitGB   *float64     `yaml:"daily_limit_gb"`
	Schedule       *rawSchedule `yaml:"schedule"`
	Jitter         *rawJitter   `yaml:"jitter_seconds"`
	CooldownHours  *int         `yaml:"cooldown_hours"`
	UserAgent      *string      `yaml:"user_agent"`
	Targets        []rawTarget  `yaml:"targets"`
}

type rawSchedule struct {
	StartHour *int `yaml:"start_hour"`
	EndHour   *int `yaml:"end_hour"`
}

type rawJitter struct {
	MinSeconds *int `yaml:"min_seconds"`
	MaxSeconds *int `yaml:"max_seconds"`
}

type rawTarget struct {
	Name    *string `yaml:"name"`
	URL     *string `yaml:"url"`
	Weight  *int    `yaml:"weight"`
	Enabled *bool   `yaml:"enabled"`
}

// DefaultConfig 返回填满默认值的配置（Targets 为空切片，非 nil）。
func DefaultConfig() *Config {
	return &Config{
		ListenAddr:     defaultListenAddr,
		StorageMode:    defaultStorageMode,
		MemoryDir:      defaultMemoryDir,
		DiskDir:        defaultDiskDir,
		MaxRateMbps:    0,
		MaxConnections: defaultMaxConnections,
		DailyLimitGB:   0,
		Schedule:       ScheduleConfig{StartHour: 0, EndHour: 24},
		Jitter:         JitterConfig{MinSeconds: defaultJitterMin, MaxSeconds: defaultJitterMax},
		CooldownHours:  defaultCooldownHours,
		UserAgent:      defaultUserAgent,
		Targets:        []TargetConfig{},
	}
}

// LoadConfig 读取并解析 YAML 配置：缺文件报错、缺字段填默认值、非法值报错。
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件 %q 失败: %w", path, err)
	}
	var raw rawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("解析配置文件 %q 失败: %w", path, err)
	}
	cfg := raw.build()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("配置文件 %q 校验失败: %w", path, err)
	}
	return cfg, nil
}

// build 依据 presence 语义装配配置：仅缺字段用默认值，显式值（含 0/false）原样保留。
func (r *rawConfig) build() *Config {
	cfg := DefaultConfig()

	if r.ListenAddr != nil {
		cfg.ListenAddr = strings.TrimSpace(*r.ListenAddr)
	}
	if r.StorageMode != nil {
		cfg.StorageMode = strings.ToLower(strings.TrimSpace(*r.StorageMode))
	}
	if r.MemoryDir != nil {
		cfg.MemoryDir = strings.TrimSpace(*r.MemoryDir)
	}
	if r.DiskDir != nil {
		cfg.DiskDir = strings.TrimSpace(*r.DiskDir)
	}
	if r.MaxRateMbps != nil {
		cfg.MaxRateMbps = *r.MaxRateMbps
	}
	if r.MaxConnections != nil {
		cfg.MaxConnections = *r.MaxConnections
	}
	if r.DailyLimitGB != nil {
		cfg.DailyLimitGB = *r.DailyLimitGB
	}
	if r.Schedule != nil {
		// 块出现后逐字段覆盖，未出现的一侧仍取默认（0 / 24）。
		cfg.Schedule = ScheduleConfig{StartHour: 0, EndHour: 24}
		if r.Schedule.StartHour != nil {
			cfg.Schedule.StartHour = *r.Schedule.StartHour
		}
		if r.Schedule.EndHour != nil {
			cfg.Schedule.EndHour = *r.Schedule.EndHour
		}
	}
	if r.Jitter != nil {
		cfg.Jitter = JitterConfig{MinSeconds: defaultJitterMin, MaxSeconds: defaultJitterMax}
		if r.Jitter.MinSeconds != nil {
			cfg.Jitter.MinSeconds = *r.Jitter.MinSeconds
		}
		if r.Jitter.MaxSeconds != nil {
			cfg.Jitter.MaxSeconds = *r.Jitter.MaxSeconds
		}
	}
	if r.CooldownHours != nil {
		cfg.CooldownHours = *r.CooldownHours
	}
	if r.UserAgent != nil {
		cfg.UserAgent = strings.TrimSpace(*r.UserAgent)
	}
	if r.Targets != nil {
		cfg.Targets = make([]TargetConfig, 0, len(r.Targets))
		for _, rt := range r.Targets {
			t := TargetConfig{Weight: defaultTargetWeight, Enabled: true}
			if rt.Name != nil {
				t.Name = strings.TrimSpace(*rt.Name)
			}
			if rt.URL != nil {
				t.URL = strings.TrimSpace(*rt.URL)
			}
			if rt.Weight != nil {
				t.Weight = *rt.Weight
			}
			if rt.Enabled != nil {
				t.Enabled = *rt.Enabled
			}
			cfg.Targets = append(cfg.Targets, t)
		}
	}
	return cfg
}

// Save 原子写入配置：先写 <path>.tmp，落盘后 os.Rename 覆盖原文件。
func (c *Config) Save(path string) error {
	if c == nil {
		return errors.New("配置为 nil，无法保存")
	}
	if err := c.Validate(); err != nil {
		return fmt.Errorf("拒绝保存非法配置: %w", err)
	}
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("序列化配置失败: %w", err)
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("创建配置目录 %q 失败: %w", dir, err)
		}
	}

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("创建临时配置文件 %q 失败: %w", tmp, err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmp)
		}
	}()

	if _, err := f.WriteString("# NetSuck 配置文件（由程序自动写入，手工编辑后重启生效）\n"); err != nil {
		_ = f.Close()
		return fmt.Errorf("写入临时配置文件失败: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("写入临时配置文件失败: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("同步临时配置文件失败: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("关闭临时配置文件失败: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("原子替换配置文件 %q 失败: %w", path, err)
	}
	committed = true
	return nil
}

// Validate 校验整份配置（保存前与加载后复用）。
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("配置为 nil")
	}
	if err := validateListenAddr(c.ListenAddr); err != nil {
		return err
	}
	if err := ValidateStorageMode(c.StorageMode); err != nil {
		return err
	}
	switch c.StorageMode {
	case storageModeMemory:
		if strings.TrimSpace(c.MemoryDir) == "" {
			return errors.New("memory_dir 不能为空（storage_mode=memory）")
		}
	case storageModeDisk:
		if strings.TrimSpace(c.DiskDir) == "" {
			return errors.New("disk_dir 不能为空（storage_mode=disk）")
		}
	}
	if err := ValidateMaxRateMbps(c.MaxRateMbps); err != nil {
		return err
	}
	if err := ValidateMaxConnections(c.MaxConnections); err != nil {
		return err
	}
	if err := ValidateDailyLimitGB(c.DailyLimitGB); err != nil {
		return err
	}
	if err := ValidateSchedule(c.Schedule); err != nil {
		return err
	}
	if err := ValidateJitter(c.Jitter); err != nil {
		return err
	}
	if c.CooldownHours <= 0 {
		return fmt.Errorf("cooldown_hours 必须 > 0，当前为 %d", c.CooldownHours)
	}
	if strings.TrimSpace(c.UserAgent) == "" {
		return errors.New("user_agent 不能为空")
	}
	for i, t := range c.Targets {
		if err := ValidateTargetInput(t.Name, t.URL, t.Weight); err != nil {
			return fmt.Errorf("targets[%d] 非法: %w", i, err)
		}
	}
	return nil
}

// ValidateStorageMode 校验存储模式枚举。
func ValidateStorageMode(v string) error {
	switch v {
	case storageModeDiscard, storageModeMemory, storageModeDisk:
		return nil
	default:
		return fmt.Errorf("storage_mode 必须为 %q/%q/%q 之一，当前为 %q",
			storageModeDiscard, storageModeMemory, storageModeDisk, v)
	}
}

// ValidateMaxRateMbps 校验全局限速：0 表示不限速，负值非法。
func ValidateMaxRateMbps(v int) error {
	if v < 0 {
		return fmt.Errorf("max_rate_mbps 不能为负（0=不限速），当前为 %d", v)
	}
	return nil
}

// ValidateMaxConnections 校验全局并发连接数：1-10。
func ValidateMaxConnections(v int) error {
	if v < 1 || v > 10 {
		return fmt.Errorf("max_connections 必须在 1-10 之间，当前为 %d", v)
	}
	return nil
}

// ValidateDailyLimitGB 校验每日配额：有限非负数（0=不限量）。
func ValidateDailyLimitGB(v float64) error {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return fmt.Errorf("daily_limit_gb 必须为有限数值，当前为 %v", v)
	}
	if v < 0 {
		return fmt.Errorf("daily_limit_gb 不能为负（0=不限量），当前为 %v", v)
	}
	return nil
}

// ValidateSchedule 校验活跃时间段：0<=start<=23、0<=end<=24，且 start != end。
func ValidateSchedule(s ScheduleConfig) error {
	if s.StartHour < 0 || s.StartHour > 23 {
		return fmt.Errorf("schedule.start_hour 必须在 0-23 之间，当前为 %d", s.StartHour)
	}
	if s.EndHour < 0 || s.EndHour > 24 {
		return fmt.Errorf("schedule.end_hour 必须在 0-24 之间，当前为 %d", s.EndHour)
	}
	if s.StartHour == s.EndHour {
		return fmt.Errorf("schedule.start_hour 与 end_hour 不能相同（当前均为 %d）", s.StartHour)
	}
	return nil
}

// ValidateJitter 校验随机休眠区间：0 <= MinSeconds <= MaxSeconds。
func ValidateJitter(j JitterConfig) error {
	if j.MinSeconds < 0 {
		return fmt.Errorf("jitter_seconds.min_seconds 不能为负，当前为 %d", j.MinSeconds)
	}
	if j.MaxSeconds < 0 {
		return fmt.Errorf("jitter_seconds.max_seconds 不能为负，当前为 %d", j.MaxSeconds)
	}
	if j.MinSeconds > j.MaxSeconds {
		return fmt.Errorf("jitter_seconds.min_seconds(%d) 不能大于 max_seconds(%d)", j.MinSeconds, j.MaxSeconds)
	}
	return nil
}

// ValidateTargetInput 校验目标输入：name 非空、URL 为 http/https 且主机名有效、weight>=1。
func ValidateTargetInput(name, rawURL string, weight int) error {
	if strings.TrimSpace(name) == "" {
		return errors.New("name 不能为空")
	}
	if err := validateTargetURL(rawURL); err != nil {
		return err
	}
	if weight < 1 {
		return fmt.Errorf("weight 必须 >= 1，当前为 %d", weight)
	}
	return nil
}

// validateTargetURL 校验下载直链：http/https、主机名非空、不含 userinfo。
func validateTargetURL(rawURL string) error {
	s := strings.TrimSpace(rawURL)
	if s == "" {
		return errors.New("url 不能为空")
	}
	u, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("url %q 解析失败: %w", rawURL, err)
	}
	if !strings.EqualFold(u.Scheme, "http") && !strings.EqualFold(u.Scheme, "https") {
		return fmt.Errorf("url %q 必须使用 http/https 协议", rawURL)
	}
	if u.User != nil {
		return fmt.Errorf("url %q 不得包含 userinfo", rawURL)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("url %q 缺少有效 hostname", rawURL)
	}
	if strings.ContainsAny(host, " \t\r\n") || strings.Contains(host, "..") {
		return fmt.Errorf("url %q 的 hostname 非法", rawURL)
	}
	return nil
}

// validateListenAddr 校验监听地址必须为 host:port 形式且端口合法。
func validateListenAddr(addr string) error {
	if strings.TrimSpace(addr) == "" {
		return errors.New("listen_addr 不能为空")
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("listen_addr %q 非法（应为 host:port）: %w", addr, err)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("listen_addr %q 端口非法", addr)
	}
	return nil
}
