from collections import defaultdict, deque
from typing import Dict, List

class DAG:
    def __init__(self):
        self.graph: Dict[str, List[str]] = defaultdict(list)
        self.in_degree: Dict[str, int] = defaultdict(int)

    def add_step(self, step_name: str, depends_on: List[str] = None):
        if depends_on is None:
            depends_on = []

        for dep in depends_on:
            self.graph[dep].append(step_name)
            self.in_degree[step_name] += 1

        if step_name not in self.in_degree:
            self.in_degree[step_name] = 0

    def topological_sort(self) -> List[str]:
        in_degree = self.in_degree.copy()
        queue = deque([node for node in in_degree if in_degree[node] == 0])
        sorted_steps = []

        while queue:
            node = queue.popleft()
            sorted_steps.append(node)
            for dependent in self.graph[node]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(sorted_steps) != len(in_degree):
            raise ValueError("DAG contains cycles or disconnected nodes")

        return sorted_steps
