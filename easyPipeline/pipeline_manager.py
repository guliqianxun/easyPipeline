# -*- coding: UTF-8 -*-
'''
@Author  : Zhiheng Liu
@Email   : visitorindark@gmail.com
@CreateDate    : 2024/11/11 15:35 
@ModifyDate    : 2024/11/11 15:35
@Desc    : pipeline Manager 管理单元，负责一整套算法的调度
'''
import logging
from typing import Dict, Any, Union
from .pipeline import Pipeline

class PipelineManager:
    """管理多个pipeline的执行和结果聚合"""
    
    def __init__(self, name: str = "pipeline_manager", logger: logging.Logger = None):
        self.name = name
        self.pipelines: Dict[str, Pipeline] = {}
        self.logger = logger if logger is not None else logging.getLogger(f"pipeline_manager.{name}")

    def add_pipeline(self, pipeline: Pipeline) -> None:
        """添加一个pipeline到管理器"""
        self.pipelines[pipeline.name] = pipeline
        self.logger.info(f"Added pipeline: {pipeline.name}")

    def remove_pipeline(self, pipeline_name: str) -> None:
        """从管理器中移除一个pipeline"""
        if pipeline_name in self.pipelines:
            del self.pipelines[pipeline_name]
            self.logger.info(f"Removed pipeline: {pipeline_name}")

    def execute_pipeline(self, pipeline_name: str, data: Any) -> Any:
        """执行指定的pipeline"""
        if pipeline_name not in self.pipelines:
            raise ValueError(f"Pipeline {pipeline_name} not found")
        
        pipeline = self.pipelines[pipeline_name]
        return pipeline.execute(data)

    def execute_all(self, data: Any) -> Dict[str, Any]:
        """执行所有注册的pipeline"""
        results = {}
        for name, pipeline in self.pipelines.items():
            try:
                self.logger.info(f"Executing pipeline: {name}")
                results[name] = pipeline.execute(data)
            except Exception as e:
                self.logger.error(f"Pipeline {name} failed: {str(e)}")
                results[name] = None
        return results

    def get_results(self) -> Dict[str, Any]:
        """获取所有pipeline的结果"""
        return {name: pipeline.get_result() for name, pipeline in self.pipelines.items()}

    def aggregate_results(self, method: str = 'all') -> Union[Dict[str, Any], Any]:
        """
        聚合多个pipeline的结果
        
        Args:
            method: 聚合方法
                   'all' - 返回所有结果的字典
                   'first' - 返回第一个有效结果
                   可以扩展添加其他聚合方法
        """
        results = self.get_results()
        
        if method == 'all':
            return results
        elif method == 'first':
            for result in results.values():
                if result is not None:
                    return result
            return None
        else:
            raise ValueError(f"Unknown aggregation method: {method}")

    def get_status(self) -> Dict:
        """获取所有pipeline的状态"""
        return {
            "manager_name": self.name,
            "total_pipelines": len(self.pipelines),
            "pipeline_statuses": {
                name: pipeline.get_status()
                for name, pipeline in self.pipelines.items()
            }
        }