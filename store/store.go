// Package store 定义工作流引擎的存储抽象层（设计文档第 4 节）。
//
// 引擎本身不依赖任何具体存储，而是依赖这里定义的接口。基础设施实现层
// （如 infra/memory）通过实现这些接口接入引擎。
package store

import (
	"context"
	"errors"

	"github.com/jninng/track/model"
)

// ErrNotFound 表示查询的资源（日志、信号、运行元数据等）不存在。
//
// Mailbox.Fetch 在没有对应信号时应返回包装了本错误的错误。
var ErrNotFound = errors.New("store: not found")

// Reader 读取指定运行实例的日志（按追加顺序返回）。
type Reader interface {
	Read(ctx context.Context, runID model.RunID) ([]model.LogEntry, error)
}

// Writer 追加一条日志。
type Writer interface {
	Append(ctx context.Context, runID model.RunID, entry model.LogEntry) error
}

// Mailbox 用于 Await 原语的外部信号存储。
//
// 信号必须持久化，直到被工作流明确消费，以解决信号竞态丢失问题
// （工作流先 Await，还是信号先到达，均不丢失）。
type Mailbox interface {
	// Push 发送信号（持久化存储）。
	Push(ctx context.Context, runID model.RunID, signal model.Signal, payload []byte) error
	// Fetch 获取信号（不删除，等待工作流确认消费）。
	// 不存在时返回 ErrNotFound。
	Fetch(ctx context.Context, runID model.RunID, signal model.Signal) ([]byte, error)
	// Ack 确认消费信号（在工作流成功记录 KindAwait 日志条目后调用）。
	Ack(ctx context.Context, runID model.RunID, signal model.Signal) error
}

// Locker 保证同一 RunID 在同一时刻只有一个 Worker 在处理。
type Locker interface {
	// Acquire 尝试获取锁。返回 true 表示成功，false 表示已被他人持有。
	Acquire(ctx context.Context, runID model.RunID) (bool, error)
	// Release 释放锁。
	Release(ctx context.Context, runID model.RunID) error
}

// Meta 管理顶层运行实例状态。
type Meta interface {
	// UpdateStatus 更新运行状态。不存在时创建。
	// 通过 opts 设置 Name/Version/Input/Output/Err 等字段。
	UpdateStatus(ctx context.Context, runID model.RunID, status model.RunStatus, opts ...MetaOption) error
	// GetResult 查询运行元数据。不存在时返回 ErrNotFound。
	GetResult(ctx context.Context, runID model.RunID) (*model.RunMeta, error)
	// ListByStatus 列出处于给定状态之一的运行实例。
	// 用于引擎的持久化兜底扫描（Recover），以恢复因进程崩溃遗留的任务。
	ListByStatus(ctx context.Context, statuses ...model.RunStatus) ([]model.RunMeta, error)
}

// Interface 是引擎所需的全部存储能力的聚合接口。
// 具体实现需同时满足 Reader/Writer/Mailbox/Locker/Meta。
type Interface interface {
	Reader
	Writer
	Mailbox
	Locker
	Meta
}

// MetaOption 用于在 UpdateStatus 中设置 RunMeta 的附加字段。
type MetaOption func(*model.RunMeta)

// WithName 设置工作流名称。
func WithName(name string) MetaOption {
	return func(m *model.RunMeta) { m.Name = name }
}

// WithVersion 设置工作流代码版本号。
func WithVersion(v string) MetaOption {
	return func(m *model.RunMeta) { m.Version = v }
}

// WithInput 设置启动参数。
func WithInput(b []byte) MetaOption {
	return func(m *model.RunMeta) { m.Input = b }
}

// WithOutput 设置最终结果。
func WithOutput(b []byte) MetaOption {
	return func(m *model.RunMeta) { m.Output = b }
}

// WithErr 设置最终错误信息。
func WithErr(e string) MetaOption {
	return func(m *model.RunMeta) { m.Err = e }
}
