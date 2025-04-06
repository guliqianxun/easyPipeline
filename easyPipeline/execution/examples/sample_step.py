import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from execution.step import Step

class MultiplyByTwoStep(Step):
    def process(self, data):
        if not isinstance(data, (int, float)):
            raise ValueError(f"Data must be number, got {type(data)}")
        return data * 2

if __name__ == "__main__":
    step = MultiplyByTwoStep("multiply_by_two")
    result = step.execute(10)
    print(f"Result: {result}")

