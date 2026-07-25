# FeedMe

**語言 / Languages:** [简体中文](README.zh-CN.md) · [繁體中文](README.zh-TW.md) · [English](../../README.md)

> FeedMe 是一個**確定性的、線性的串流資料管線引擎**，圍繞記憶體、順序與失敗處理提供機械性保證。

FeedMe 適用於需要可靠資料攝取的 Rust 應用程式。它強調有界資源使用、明確的錯誤處理，以及不影響執行的全面可觀測性。

---

## 關於 (About)

FeedMe 提供線性的、確定性的處理模型：

- **串流、有界記憶體**：一次處理一個事件；記憶體佔用保持平穩。
- **確定性處理**：相同的輸入 + 相同的組態 → 相同的輸出。
- **結構化錯誤**：每次失敗都帶有階段（stage）、錯誤碼（code）與訊息（message）。
- **可觀測性**：指標可匯出（Prometheus 或 JSON），且不影響執行。
- **可擴充**：透過定義的外掛契約新增自訂階段。

---

## 主要特性 (Key Features)

- **串流、有界記憶體**：一次處理一個事件；記憶體使用與輸入大小無關、保持平坦。
- **確定性處理**：相同輸入 + 相同組態必然產生相同輸出。
- **結構化錯誤**：每個階段、錯誤碼與訊息對應一次失敗。
- **可觀測性**：指標可匯出（Prometheus 或 JSON），不影響執行。
- **可擴充**：透過已定義的外掛契約新增自訂階段。

---

## 機械性保證 (Mechanical Guarantees)

FeedMe 提供以下機械性保證：

- 事件嚴格按照輸入順序處理。
- 記憶體使用有界且與輸入大小無關。
- 階段不能觀察到共享或被修改的狀態。
- **驗證失敗不會被靜默忽略**：任何驗證器階段（RequiredFields、TypeChecking、ValueConstraints）都會拒絕無效事件。這是**選擇啟用**的——只有存在此類階段時才成立。呼叫 `Pipeline::require_validation(true)` 可將其強制為**失敗即關閉（fail-closed）**：若沒有驗證器階段，`process_event` 會回傳錯誤，而不是靜默放行無效事件。
- 指標收集不會影響執行。

---

## 為什麼 FeedMe 不是 X (Why FeedMe is Not X)

### 不是分散式（如 Vector 或 Fluent Bit）
單行程、單執行緒、本機執行。沒有網路協調或分散式 offset。

### 不是有狀態（如傳統 ETL 工具）
每個事件獨立處理，階段之間不共享可變狀態。

### 不是非同步優先（如許多現代 Rust 函式庫）
執行是同步、線性的。沒有 async runtime。

### 不是 DSL（如 Logstash）
你用 Rust 撰寫階段；沒有需要學習的單獨組態語言。

### 不是守護程式（如 filebeat）
它是一個函式庫，內嵌到你的二進位檔中。

---

## 不變量 (Invariants)

FeedMe 強制實施**機械性的行為保證**，透過執行期斷言與契約測試進行驗證。關鍵保證包括：

- **指標純粹性**：匯出指標不會影響管線狀態。
- **丟棄計數規則**：事件僅在特定條件下才計入「已丟棄」。
- **延遲記錄**：成功的階段執行始終記錄耗時。
- **目錄確定性**：相同的目錄輸入產生相同的輸出。

完整的契約測試套件見 [src/invariant_contracts.rs](../../src/invariant_contracts.rs)。

---

## 安裝 (Installation)

在 `Cargo.toml` 中加入：

```toml
[dependencies]
feedme = "0.3"
serde_json = "1.0"
regex = "1.0"
```

或透過 cargo 安裝：

```bash
cargo add feedme serde_json regex
```

---

## 快速開始 (Quick Start)

下面是一個完整可執行的管線：選擇欄位 → 脫敏 PII → 必填驗證 → 過濾 → 輸出。

```rust
use feedme::{
    Pipeline, FieldSelect, RequiredFields, StdoutOutput, Deadletter,
    PIIRedaction, Filter, InputSource, Stage,
};
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    // 建立管線：選擇欄位 → 脫敏 PII → 必填欄位 → 過濾 → 輸出
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

    // 錯誤死信（deadletter）
    let mut deadletter = Deadletter::new(PathBuf::from("samples/errors.ndjson"));

    // 處理輸入檔案
    let mut input = InputSource::File(PathBuf::from("samples/input.ndjson"));
    input.process_input(&mut pipeline, &mut Some(&mut deadletter))?;

    // 匯出最終指標
    for metric in pipeline.export_json_logs() {
        println!("{}", serde_json::to_string(&metric)?);
    }

    Ok(())
}
```

### 強制驗證（失敗即關閉）

```rust
use feedme::{Pipeline, RequiredFields};

let mut pipeline = Pipeline::new().require_validation(true);
pipeline.add_stage(Box::new(RequiredFields::new(vec!["level".to_string()])));
// 若沒有驗證器階段，process_event 會立即回傳錯誤。
```

---

## 確定性驗證 (Determinism Verification)

FeedMe 是確定性的：相同的輸入與組態必然產生相同的輸出。

```bash
cargo run --example 01_redact_validate_deadletter > run1.out
cargo run --example 01_redact_validate_deadletter > run2.out
# Unix:
sha256sum run1.out run2.out
# Windows:
certutil -hashfile run1.out SHA256 && certutil -hashfile run2.out SHA256
```

兩次執行的雜湊值應當一致。

---

## 範例 (Examples)

```bash
cargo run --example 01_redact_validate_deadletter
```

可用範例：

| 範例 | 內容 |
| --- | --- |
| `01_redact_validate_deadletter` | 脫敏 + 驗證 + 死信 |
| `02_filter_warn_error` | 過濾、警告與錯誤 |
| `03_field_projection` | 欄位投影 |
| `04_directory_ingest` | 目錄攝取 |
| `05_custom_stage` | 自訂階段 |
| `06_syslog_parsing` | syslog 解析 |
| `07_metrics_export_demo` | 指標匯出示範 |
| `08_stdin_streaming` | 標準輸入串流處理 |
| `09_complex_pipeline` | 複雜管線 |
| `10_plugin_usage` | 外掛使用 |
| `11_config_driven_pipeline` | 組態驅動的管線 |
| `12_error_handling_variations` | 錯誤處理變體 |
| `13_performance_baseline_guard` | 效能基線守護 |
| `14_fault_injection_testing` | 故障注入測試 |
| `15_audit_trail` | 稽核追蹤 |
| `16_pipeline_evolution` | 管線演進 |
| `17_fused_rule_engine` | 融合規則引擎 |
| `18_derived_and_remap` | 派生與重對映 |
| `19_constraints_and_types` | 約束與型別 |
| `20_file_output` | 檔案輸出 |
| `21_common_pipeline` | 通用管線預設 |
| `99_failure_tour` | 失敗全流程示範 |

---

## API 概覽 (API Overview)

### 階段契約 (Stage Contract)

```rust
pub trait Stage {
    fn execute(&mut self, event: Event) -> Result<Option<Event>, PipelineError>;
    fn name(&self) -> &str;
    fn is_output(&self) -> bool { false }  // true 表示消費事件
}
```

### 執行語意 (Execution Semantics)

- `Some(event)`：傳遞給下一階段。
- `None`：丟棄（被過濾）；若是 `is_output()` 則為已消費。
- `Err`：停止管線，回傳帶歸因的錯誤。

### 核心型別 (Core Types)

- `Pipeline`：新增階段、處理事件、匯出指標。
- `Event`：JSON 資料 + 選用中繼資料。
- `InputSource`：從 stdin / 檔案 / 目錄串流讀取。
- `PipelineError`：分類錯誤（Parse / Transform / Validation / Output / System）。

---

## 模糊測試 (Fuzzing)

`fuzz/` 目錄是一個獨立的 [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) 工作區，被**排除**在主工作區之外，並且需要 **nightly** 工具鏈與 `cargo-fuzz` 子命令：

```bash
# 一次性安裝模糊測試工具
cargo +nightly install cargo-fuzz

# 建置所有模糊目標
cargo +nightly fuzz build

# 執行某個目標（有限次執行）
cargo +nightly fuzz run fuzz_pipeline -- -runs=1000
```

模糊測試**不會**在 CI 中自動執行，它是面向貢獻者的、選用的本機活動。穩定的 `cargo test`、clippy 與 fmt 仍是 CI 中唯一的門禁。

---

## 適用範圍與限制 (Scope and Limits)

FeedMe 的保證是精確但**狹窄**的，僅適用於它實際實作的模型——單行程、單執行緒、本機、有序的串流處理器：

- **單執行緒**：一次處理一個事件，依輸入順序。沒有並發、平行或 async。吞吐量受限於單一核心。
- **僅本機輸入**：來源為 `stdin`、單一檔案或非遞歸目錄（路徑被讀入一個有序的 `Vec`）。沒有網路、沒有遠端 offset、沒有分散式協調。
- **無背壓（backpressure）**：輸入逐行讀取並立即處理；引擎不緩衝無限輸入，也不做流控。「有界記憶體」指每個事件的工作狀態是平坦的，而非無限輸入被分頁落盤。
- **範圍之外**：網路、async runtime、持久化、查詢語言與水平擴充。所謂「一次做對」指的是這一本地模型的正確性，而非分散式或高吞吐部署。
- **模糊測試需要 nightly**：`fuzz/` 目標需要 nightly 工具鏈與 `cargo-fuzz`（見上文）。

---

## 來源說明 (Provenance)

程式碼庫的相當部分（包含若干 `[autonomous phaseN]` 合併提交）由 AI 代理生成，而非由人類作者端到端撰寫。這些部分**並未**全部經過單獨的人工審查標記。請將其視為可用的原型：CI 中強制執行測試、clippy 與格式化門禁，但這不應被解讀為正確性保證。修改由 AI 生成的程式碼時，請用現有測試驗證行為，並為任何改動補充覆蓋。

---

## 貢獻 (Contributing)

歡迎貢獻！詳見 [貢獻指南 (CONTRIBUTING.md)](../../CONTRIBUTING.md)。

### 開發環境建置

```bash
git clone https://github.com/Michael-A-Kuykendall/feedme.git
cd feedme
cargo build
cargo test
```

### 模糊測試

FeedMe 在 `fuzz/` 下提供模糊目標（獨立的 cargo-fuzz 工作區，排除在主工作區之外）。本機執行需要 **nightly** 工具鏈與 `cargo-fuzz` 子命令：

```bash
cargo +nightly install cargo-fuzz
cd fuzz
cargo +nightly fuzz build
cargo +nightly fuzz run fuzz_pipeline -- -runs=1000
```

模糊測試**不**在 CI 中自動執行——它是選用、本機的。穩定的 `cargo test`、clippy 與 fmt 仍是唯一的 CI 門禁。

---

## 授權 (License)

基於 MIT 授權發佈。詳見 [LICENSE](../../LICENSE)。
