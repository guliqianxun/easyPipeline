# EasyPipeline 中文文档

> 一个轻量级、状态驱动的 Python 任务执行框架，支持基于 DAG 的流程调度、多进程并发调度与步骤级监控。

---

## 🚀 项目简介

**EasyPipeline** 是一个面向工程实践的开源流程执行框架，采用 **状态驱动架构**，在支持并发执行的同时保持结构清晰、状态可追踪、易于拓展。

---

## 🧱 核心组成模块

| 层级 | 模块 | 说明 |
|------|------|------|
| 执行层 | `Step`、`Pipeline` | 执行最小单元与其组合逻辑（支持 DAG） |
| 调度层 | `TaskScheduler` | 任务分发与执行策略控制 |
| 管理层 | `PipelineManager` | 注册流程、调度执行、查询状态与结果 |
| 状态模型 | `ScheduledTask`, `TaskMetadata` | 标准化执行状态与运行中数据结构 |

---

## 🌉 架构概览


- Pipeline 由多个 Step 组成
- 每个 Step 会更新自身运行状态，如 `status_flag`, `metrics`, `duration`
- Pipeline 内执行顺序由 DAG 控制
- TaskScheduler 统一调度并支持线程 / 进程 / 批处理
- PipelineManager 负责管理注册、调度、查询

### 支持的执行模式

- `SEQUENTIAL`：顺序执行（适用于测试/小任务）
- `PARALLEL_THREAD`：多线程执行（共享内存场景）
- `PARALLEL_PROCESS`：多进程隔离执行（适用于计算密集任务）
- `BATCH`：批次数据并行调度

---

## 📦 框架特性

- ✅ DAG 流程定义与依赖执行
- ✅ 多种并发模型支持
- ✅ Step / Pipeline 多级运行指标记录
- ✅ 状态驱动设计，适用于多进程环境
- ✅ 插件式架构，支持用户拓展回调、日志、存储

---

## 📂 目录结构

```
/manager
  ├─ pipeline_manager.py        # 管理器入口
  ├─ task_scheduler.py          # 调度器核心
  ├─ scheduled.py               # 任务元数据结构
/pipeline
  ├─ pipeline.py                # 流水线执行控制
  ├─ step.py                    # Step 抽象基类
  ├─ dag.py                     # DAG 拓扑执行顺序解析
/commom
  └─ metrics.py                 # 指标容器与聚合
  └─ types.py                   # 类型定义
  └─ retry.py                   # 重试策略
```

---

## 🧪 快速上手


### 示例：串行执行
```python
from pipeline.pipeline import Pipeline
from pipeline.step import Step
from manager.pipeline_manager import PipelineManager

class MultiplyStep(Step):
    def run(self, x):
        return x * 2

pipe = Pipeline(name="double")
pipe.add_step(MultiplyStep("step1"))
pipe.add_step(MultiplyStep("step2"), depends_on=["step1"])

manager = PipelineManager()
manager.register_pipeline(pipe)
manager.execute({"double": [10, 20]})

print(manager.get_status("double"))
print(manager.get_result("double"))
```

### 示例：多进程执行（PARALLEL_PROCESS）
```python
from manager.pipeline_manager import PipelineManager
from manager.task_scheduler import ExecutionMode

# 同样使用上面的 Pipeline 构建逻辑
...

manager = PipelineManager(execution_mode=ExecutionMode.PARALLEL_PROCESS)
manager.register_pipeline(pipe)
manager.execute({"double": [100, 200, 300, 400]})

import time
while True:
    status = manager.get_status("double")
    print("进度:", status)
    if status.get("completed") == status.get("total"):
        break
    time.sleep(1)

results = manager.get_result("double")
print("结果:", results)
```

---

## 📊 状态追踪与可观测性

所有任务状态、执行进度和指标将通过 `TaskMetadata` 记录，如：

```json
{
  "status_flag": "success",
  "metrics": { "duration": 0.005 },
  "message": null,
  "progress": [2, 5],
  "last_updated": "2024-04-06T13:42:00"
}
```

你可以将这些状态：
- 写入 JSON 文件
- 存储至 SQLite / Redis
- 输出为 Prometheus 格式指标
- 用于构建 Web UI / CLI 可视化界面

---

## 🔧 开发规划（Roadmap）

- [ ] 支持 RESTful 接口触发任务与查询
- [ ] 构建 Web 看板（状态流、日志、进度）
- [ ] 集成 Prometheus exporter
- [ ] 步骤级重试控制器
- [ ] 缓存中间输出 + 持久化策略

---

## 👥 社区与贡献

欢迎你提交 PR、使用反馈或提 Issue，EasyPipeline 致力于打造真正轻量而现代的工作流框架。


