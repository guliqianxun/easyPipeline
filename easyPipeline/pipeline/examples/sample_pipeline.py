import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from pipeline.pipeline import Pipeline
from execution.examples.sample_step import MultiplyByTwoStep

import logging
logging.basicConfig(level=logging.INFO)

pipeline = Pipeline(name="SamplePipeline")

# Step 注册
step1 = MultiplyByTwoStep("multiply1")
step2 = MultiplyByTwoStep("multiply2")

# 构建 pipeline 的 DAG
pipeline.add_step(step1)  # 无依赖
pipeline.add_step(step2, depends_on=["multiply1"])

# 运行 pipeline
final_result = pipeline.run(10)

print(f"Pipeline final result: {final_result}")
print(f"Metrics: {pipeline.get_metrics()}")
