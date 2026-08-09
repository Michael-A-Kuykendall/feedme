# FeedMe

**简体中文** · [繁體中文](../zh-TW/README.md) · [English](../../README.md)

> FeedMe 是一个**确定性的、线性的流式数据管道引擎**，围绕内存、顺序与失败处理提供机械性保证。

FeedMe 适用于需要可靠数据摄取的 Rust 应用程序。它强调有界资源使用、显式的错误处理，以及不影响执行的全面可观测性。

---

## 关于 (About)

FeedMe 提供线性的、确定性的处理模型：

- **流式、有界内存**：一次处理一个事件；内存占用保持平稳。
- **确定性处理**：相同的输入 + 相同的配置 → 相同的输出。
- **结构化错误**：每次失败都带有阶段（stage）、错误码（code）和消息（message）。
- **可观测性**：指标可导出（Prometheus 或 JSON），且不影响执行。
- **可扩展**：通过定义的插件契约添加自定义阶段。

---

## 主要特性 (Key Features)

- **流式、有界内存**：一次处理一个事件；内存使用与输入大小无关、保持平坦。
- **确定性处理**：相同输入 + 相同配置必然产生相同输出。
- **结构化错误**：每个阶段、错误码与消息对应一次失败。
- **可观测性**：指标可导出（Prometheus 或 JSON），不影响执行。
- **可扩展**：通过已定义的插件契约添加自定义阶段。

---

## 机械性保证 (Mechanical Guarantees)

FeedMe 提供以下机械性保证：

- 事件严格按照输入顺序处理。
- 内存使用有界且与输入大小无关。
- 阶段不能观察到共享或被修改的状态。
- **验证失败不会被静默忽略**：任何验证器阶段（RequiredFields、TypeChecking、ValueConstraints）都会拒绝无效事件。这是**选择启用**的——只有存在此类阶段时才成立。调用 `Pipeline::require_validation(true)` 可将其强制为**失败即关闭（fail-closed）**：若没有验证器阶段，`process_event` 会返回错误，而不是静默放行无效事件。
- 指标收集不会影响执行。

---

## 为什么 FeedMe 不是 X (Why FeedMe is Not X)

### 不是分布式（如 Vector 或 Fluent Bit）
单进程、单线程、本地执行。没有网络协调或分布式 offset。

### 不是有状态（如传统 ETL 工具）
每个事件独立处理，阶段之间不共享可变状态。

### 不是异步优先（如许多现代 Rust 库）
处理核心在设计上是同步的——这是一项特性，而非限制。异步仅保留给 I/O 边缘适配器（数据源/输出端）；它永远不会进入阶段（stage）循环（详见下方"适用范围与限制"）。

### 不是 DSL（如 Logstash）
你用 Rust 编写阶段；没有单独的配置语言需要学习。

### 不是守护进程（如 filebeat）
它是一个库，嵌入到你的二进制中。

---

## 不变量 (Invariants)

FeedMe 强制实施**机械性的行为保证**，通过运行时断言和契约测试进行验证。关键保证包括：

- **指标纯粹性**：导出指标不会影响管道状态。
- **丢弃计数规则**：事件仅在特定条件下才计入“已丢弃”。
- **延迟记录**：成功的阶段执行始终记录耗时。
- **目录确定性**：相同的目录输入产生相同的输出。

完整的契约测试套件见 [src/invariant_contracts.rs](../../src/invariant_contracts.rs)。

---

## 安装 (Installation)

在 `Cargo.toml` 中添加：

```toml
[dependencies]
feedme = "0.5"
serde_json = "1.0"
regex = "1.0"
```

或通过 cargo 安装：

```bash
cargo add feedme serde_json regex
```

---

## 快速开始 (Quick Start)

下面是一个完整可运行的管道：选择字段 → 脱敏 PII → 必填校验 → 过滤 → 输出。

```rust
use feedme::{
    Pipeline, FieldSelect, RequiredFields, StdoutOutput, Deadletter,
    PIIRedaction, Filter, InputSource, Stage,
};
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    // 创建管道：选择字段 → 脱敏 PII → 必填字段 → 过滤 → 输出
    let mut pipeline = Pipeline::new();
    pipeline.add_stage(Box::new(FieldSelect::new(vec![
        "timestamp".into(), "level".into(), "message".into(), "email".into()
    ])));
    let email_pattern = regex::Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b")?;
    pipeline.add_stage(Box::new(PIIRedaction::new(vec![email_pattern])));
    pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".into()])));
    pipeline.add_stage(Box::new(Filter::new(Box::new(|event| {
        event.data.get("level").and_then(|v| v.as_str()) != Some("debug")
    }))));
    pipeline.add_stage(Box::new(StdoutOutput::new()));

    // 错误死信（deadletter）
    let mut deadletter = Deadletter::new(PathBuf::from("samples/errors.ndjson"));

    // 处理输入文件
    let mut input = InputSource::File(PathBuf::from("samples/input.ndjson"));
    input.process_input(&mut pipeline, &mut Some(&mut deadletter))?;

    // 导出最终指标
    for metric in pipeline.export_json_logs() {
        println!("{}", serde_json::to_string(&metric)?);
    }

    Ok(())
}
```

### 强制验证（失败即关闭）

```rust
use feedme::{Pipeline, RequiredFields};

let mut pipeline = Pipeline::new().require_validation(true);
pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));
// 若没有验证器阶段，process_event 会立即返回错误。
```

---

## 确定性验证 (Determinism Verification)

FeedMe 是确定性的：相同的输入与配置必然产生相同的输出。

```bash
cargo run --example 01_redact_validate_deadletter > run1.out
cargo run --example 01_redact_validate_deadletter > run2.out
# Unix:
sha256sum run1.out run2.out
# Windows:
certutil -hashfile run1.out SHA256 && certutil -hashfile run2.out SHA256
```

两次运行的哈希值应当一致。

> **说明**：此流程验证的是已处理事件流。指标导出（`export_json_logs`/`export_prometheus`）的确定性已单独验证——它有自己的排序问题（阶段/丢弃原因条目的顺序），与事件输出顺序无关；详见 `src/tests.rs` 中的 `test_metrics_export_ordering_is_deterministic`。

---

## 示例 (Examples)

```bash
cargo run --example 01_redact_validate_deadletter
```

可用示例：

| 示例 | 内容 |
| --- | --- |
| `01_redact_validate_deadletter` | 脱敏 + 校验 + 死信 |
| `02_filter_warn_error` | 过滤、警告与错误 |
| `03_field_projection` | 字段投影 |
| `04_directory_ingest` | 目录摄取 |
| `05_custom_stage` | 自定义阶段 |
| `06_syslog_parsing` | syslog 解析 |
| `07_metrics_export_demo` | 指标导出演示 |
| `08_stdin_streaming` | 标准输入流式处理 |
| `09_complex_pipeline` | 复杂管道 |
| `10_plugin_usage` | 插件使用 |
| `11_config_driven_pipeline` | 配置驱动的管道 |
| `12_error_handling_variations` | 错误处理变体 |
| `13_performance_baseline_guard` | 性能基线守护 |
| `14_fault_injection_testing` | 故障注入测试 |
| `15_audit_trail` | 审计追踪 |
| `16_pipeline_evolution` | 管道演进 |
| `17_fused_rule_engine` | 融合规则引擎 |
| `18_derived_and_remap` | 派生与重映射 |
| `19_constraints_and_types` | 约束与类型 |
| `20_file_output` | 文件输出 |
| `21_common_pipeline` | 通用管道预设 |
| `99_failure_tour` | 失败全流程演示 |

---

## API 概览 (API Overview)

### 阶段契约 (Stage Contract)

```rust
pub trait Stage {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError>;
    fn name(&self) -> &str;
    fn is_output(&self) -> bool { false }  // true 表示消费事件
}
```

### 执行语义 (Execution Semantics)

- `Some(event)`：传递给下一阶段。
- `None`：丢弃（被过滤）；若是 `is_output()` 则为已消费。
- `Err`：停止管道，返回带归因的错误。

### 核心类型 (Core Types)

- `Pipeline`：添加阶段、处理事件、导出指标。
- `Event`：JSON 数据 + 可选元数据。
- `InputSource`：从 stdin / 文件 / 目录流式读取。
- `PipelineError`：分类错误（Parse / Transform / Validation / Output / System）。

---

## 模糊测试 (Fuzzing)

`fuzz/` 目录是一个独立的 [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) 工作区，被**排除**在主工作区之外，并且需要 **nightly** 工具链与 `cargo-fuzz` 子命令：

```bash
# 一次性安装模糊测试工具
cargo +nightly install cargo-fuzz

# 构建所有模糊目标
cargo +nightly fuzz build

# 运行某个目标（有限次执行）
cargo +nightly fuzz run fuzz_pipeline -- -runs=1000
```

模糊测试**不会**在 CI 中自动运行，它是面向贡献者的、可选的本地活动。稳定的 `cargo test`、clippy 和 fmt 仍是 CI 中唯一的门禁。

---

## 适用范围与限制 (Scope and Limits)

FeedMe 的定位是刻意收窄的：它是**确定性的、有界的、同步核心的数据管道库**——线性、可嵌入。它将杂乱的输入流转化为干净、有序事件，服务于下游系统或任意输出端。它*不是*关系型/聚合引擎、分布式系统，也不是守护进程。

其保证适用于它实际实现的模型——单进程、本地、有序的流处理器：

- **同步处理核心**：阶段管道单线程运行，按输入顺序执行。这是一项**特性**，而非限制——这与成熟的同步引擎为保证确定性与有界内存所做的架构选择相同。阶段循环内部没有并发。
- **仅在边缘使用异步**：实现 `InputSourceAdapter` 特质（trait）的类型，或实现 `OutputSink` 的 `Stage`，可以在内部执行异步 I/O（网络、流式传输）。管道*协调*部分保持同步；异步永远不会进入阶段循环。这是 Kafka/S3/HTTP 摄取功能计划使用的扩展点；具体的 trait 契约见 [`docs/decisions/0001-input-output-async-edges.md`](../decisions/0001-input-output-async-edges.md)。
- **当前仅支持本地输入**：内置数据源为 `stdin`、单个文件，或非递归目录（路径被读入一个有序的 `Vec`）。目前尚无远程 offset 或分布式协调。
- **无背压（backpressure）**：输入逐行读取并立即处理；引擎不缓冲无限输入，也不做流控。”有界内存”指每个事件的工作状态是平坦的，而非无限输入被分页落盘。
- **范围之外**：分布式处理、持久化存储、查询语言、压缩/加密，以及关系型聚合/连接（多事件分析不在本库范围内）。“一次做对”指的是这一本地模型的正确性，而非分布式或高吞吐部署。
- **模糊测试需要 nightly**：`fuzz/` 目标需要 nightly 工具链与 `cargo-fuzz`（见上文）。

---

## 来源说明 (Provenance)

代码库的相当部分（包括若干 `[autonomous phaseN]` 合并提交）由 AI 代理生成，而非由人类作者端到端编写。这些部分**并未**全部经过单独的人工审查标记。请将其视为可用的原型：CI 中强制执行测试、clippy 与格式化门禁，但这不应被解读为正确性保证。修改由 AI 生成的代码时，请用现有测试验证行为，并为任何改动补充覆盖。

---

## 贡献 (Contributing)

欢迎贡献！详见 [贡献指南 (CONTRIBUTING.md)](../../CONTRIBUTING.md)。

### 开发环境搭建

```bash
git clone https://github.com/Michael-A-Kuykendall/feedme.git
cd feedme
cargo build
cargo test
```

### 模糊测试

FeedMe 在 `fuzz/` 下提供模糊目标（独立的 cargo-fuzz 工作区，排除在主工作区之外）。本地运行需要 **nightly** 工具链与 `cargo-fuzz` 子命令：

```bash
cargo +nightly install cargo-fuzz
cd fuzz
cargo +nightly fuzz build
cargo +nightly fuzz run fuzz_pipeline -- -runs=1000
```

模糊测试**不**在 CI 中自动运行——它是可选、本地的。稳定的 `cargo test`、clippy 和 fmt 仍是唯一的 CI 门禁。

---

## 许可证 (License)

基于 MIT 或 Apache-2.0 双许可证发布，由你选择。详见 [LICENSE-MIT](../../LICENSE-MIT) / [LICENSE-APACHE](../../LICENSE-APACHE)。
