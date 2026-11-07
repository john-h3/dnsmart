# dnsmart 项目约定

## Go 后端（backend/）

`backend/` 下的 Go 代码模块名为 `dnsmart`，所有 Go 代码都必须遵循 Effective Go（https://go.dev/doc/effective_go）以及 Go 官方的 Code Review Comments 约定。

### 项目结构

- 启动入口放在 `backend/cmd/<应用名>/main.go`，例如 `backend/cmd/server/main.go`。
- 业务代码放在 `backend/internal/` 下，按职责划分包，外部模块无法导入。
- `backend/` 根目录不放 `main.go`，不放构建产物（二进制）。
- 不要预先创建空目录，需要时再建。

### 格式与命名

- 所有代码必须经过 `gofmt`，不手动调整缩进和空格。
- 使用 MixedCaps 或 mixedCaps 命名，不用下划线。
- 包名简短、小写、单词形式，不用复数，不与标准库冲突。避免 `util`、`common` 这类泛化包名。
- 导出的 getter 不加 `Get` 前缀，例如字段 `owner` 的方法名是 `Owner()`。
- 接口名以 `-er` 结尾（如 `Reader`），单方法接口优先。
- 导出的标识符必须有以名称开头的文档注释。

### 错误处理

- 每个返回的 error 都必须检查，不用 `_` 忽略。
- 错误用 `fmt.Errorf("...: %w", err)` 包装，保留调用链。
- 错误信息小写开头，不以标点结尾。
- 不用 `panic` 处理可预期的错误，`panic` 只用于程序无法继续的情况。
- 哨兵错误用 `var ErrXxx = errors.New(...)`，判断用 `errors.Is` / `errors.As`。

### 接口与类型设计

- 接口由使用方定义，而不是由实现方定义。
- 函数接收接口，返回具体类型（accept interfaces, return structs）。
- 设计零值可用的类型，减少必须的构造步骤。
- 不为了测试而过早抽象接口。

### 并发与上下文

- 所有可能阻塞或耗时的操作都接收 `context.Context` 作为第一个参数。
- 启动的 goroutine 必须有明确的退出方式，避免泄漏。
- 通过通信共享内存，而不是通过共享内存通信。能用 channel 清楚表达时用 channel，否则用 `sync` 包。

### 其他约定

- 资源释放使用 `defer`，并紧跟在获取资源之后。
- 优先使用 `:=` 声明，零值变量用 `var`。
- 不写无意义的注释，注释解释「为什么」而不是「做什么」。
- 只在需要时添加功能，不预先做抽象和配置项。

### 提交前检查

每次修改 Go 代码后，在 `backend/` 下依次运行，全部通过才算完成：

```
gofmt -l .
go vet ./...
go build ./...
```

`gofmt -l .` 不应输出任何文件名。如果有测试，还需运行 `go test ./...`。
