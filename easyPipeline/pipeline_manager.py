# -*- coding: UTF-8 -*-
'''
@Author  : Zhiheng Liu
@Email   : visitorindark@gmail.com
@CreateDate    : 2024/11/11 15:35 
@ModifyDate    : 2024/11/11 15:35
@Desc    : pipeline Manager 管理单元，负责一整套算法的调度
'''
import logging
from datetime import datetime
from typing import Dict, Any, Union, List, Optional
from .pipeline import Pipeline
from multiprocessing import Pool
import pickle
from .types import StepStatus, MetricsData
import os


def _pipeline_worker(args):
    """Worker function to run pipeline instance with specific input"""
    pipeline_pickle, input_data_list, chunk_id = args
    
    if not isinstance(input_data_list, list):
        input_data_list = [input_data_list]
        
    results = []
    pipeline = pickle.loads(pipeline_pickle)
    logger = logging.getLogger(f"pipeline.{pipeline.name}.{chunk_id}")
    pipeline.logger = logger
    
    logger.info(f"Starting chunk {chunk_id} with {len(input_data_list)} items")
    
    for i, input_data in enumerate(input_data_list):
        try:
            result = pipeline.execute(input_data)
            results.append({
                'instance_id': f"{chunk_id}_item_{i}",
                'input_data': input_data,
                'status': pipeline.get_status(),
                'result': result,
                'metrics': pipeline.metrics,
                'success': True,
                'error': None
            })
        except Exception as e:
            results.append({
                'instance_id': f"{chunk_id}_item_{i}",
                'input_data': input_data,
                'status': {
                    'pipeline_name': pipeline.name,
                    'status': StepStatus.FAILED.value,
                    'error': str(e)
                },
                'result': None,
                'metrics': MetricsData(
                    start_time=datetime.now(),
                    end_time=datetime.now(),
                    custom_metrics={'error': str(e)}
                ),
                'success': False,
                'error': str(e)
            })
            logger.error(f"Failed processing item {i} in chunk {chunk_id}: {e}")
    
    return results

class PipelineManager:
    """Manages the execution and aggregation of multiple pipelines"""
    
    def __init__(self, name: str = "pipeline_manager", logger: logging.Logger = None):
        self.name = name
        self.pipelines: Dict[str, Pipeline] = {}
        self.logger = logger if logger is not None else logging.getLogger(f"pipeline_manager.{name}")
        # To track execution metrics
        self.execution_metrics: Dict[str, Dict[str, Union[int, float]]] = {
            # Example format: 'pipeline_name': {'call_count': 0, 'total_duration': 0.0}
        }

    def add_pipeline(self, pipeline: Pipeline) -> None:
        """Add a pipeline to the manager"""
        self.pipelines[pipeline.name] = pipeline
        self.execution_metrics[pipeline.name] = {'call_count': 0, 'total_duration': 0.0}
        self.logger.info(f"Added pipeline: {pipeline.name}")

    def remove_pipeline(self, pipeline_name: str) -> None:
        """Remove a pipeline from the manager"""
        if pipeline_name in self.pipelines:
            del self.pipelines[pipeline_name]
            del self.execution_metrics[pipeline_name]
            self.logger.info(f"Removed pipeline: {pipeline_name}")

    def execute_pipeline(self, pipeline_name: str, data: Any) -> Any:
        """Execute the specified pipeline and track its metrics"""
        if pipeline_name not in self.pipelines:
            raise ValueError(f"Pipeline {pipeline_name} not found")
        
        pipeline = self.pipelines[pipeline_name]

        
        result = pipeline.execute(data)  # Execute the pipeline
        duration = pipeline.metrics.duration_seconds
        
        self.execution_metrics[pipeline_name]['call_count'] += 1
        self.execution_metrics[pipeline_name]['total_duration'] += duration
        return result

    def execute_all(self, data: Any) -> Dict[str, Any]:
        """Execute all registered pipelines and track their metrics"""
        results = {}
        for name, pipeline in self.pipelines.items():
            try:
                self.logger.info(f"Executing pipeline: {name}")
                results[name] = self.execute_pipeline(name, data)
            except Exception as e:
                self.logger.error(f"Pipeline {name} failed: {str(e)}")
                results[name] = None
        return results

    def get_results(self) -> Dict[str, Any]:
        """Get results from all pipelines"""
        return {name: pipeline.get_result() for name, pipeline in self.pipelines.items()}

    def summarize_metrics(self) -> Dict[str, Any]:
        """
        Summarize pipeline execution metrics.

        Returns:
            A dictionary containing:
            - Total pipelines called
            - Total execution time across all pipelines
            - Mean execution time per pipeline
        """
        total_pipelines = len(self.execution_metrics)
        total_duration = sum(metrics['total_duration'] for metrics in self.execution_metrics.values())
        total_calls = sum(metrics['call_count'] for metrics in self.execution_metrics.values())
        mean_duration = total_duration / total_calls if total_calls > 0 else 0.0

        summary = {
            "total_pipelines": total_pipelines,
            "total_calls": total_calls,
            "total_duration": total_duration,
            "mean_duration": mean_duration,
        }

        self.logger.info(f"Pipeline Metrics Summary: {summary}")
        return summary

    def get_status(self) -> Dict:
        """Get the current status of all pipelines and their metrics"""
        return {
            "manager_name": self.name,
            "total_pipelines": len(self.pipelines),
            "pipeline_statuses": {
                name: pipeline.get_status()
                for name, pipeline in self.pipelines.items()
            },
            "execution_metrics": self.execution_metrics
        }

    def log_pipeline_metrics(self) -> None:
        """Log metrics for each pipeline"""
        self.logger.info("Pipeline Execution Metrics:")
        for name, metrics in self.execution_metrics.items():
            self.logger.info(
                f"Pipeline: {name}, Call Count: {metrics['call_count']}, Total Duration: {metrics['total_duration']:.2f}s"
            )


class BatchPipelineManager:
    """Manager for parallel execution of pipeline instances with different inputs"""
    
    def __init__(self, name: str = "batch_pipeline_manager",
                 max_workers: Optional[int] = None,
                 logger: Optional[logging.Logger] = None):
        self.name = name
        self.max_workers = max_workers or os.cpu_count()
        self.pipelines: Dict[str, Pipeline] = {}
        self.logger = logger or logging.getLogger(f"pipeline_manager.{name}")
        self.execution_metrics: Dict[str, Dict] = {}

    def add_pipeline(self, pipeline: Pipeline) -> None:
        """Register a pipeline template"""
        self.pipelines[pipeline.name] = pipeline
        self.execution_metrics[pipeline.name] = {
            'total_instances': 0,
            'successful_instances': 0,
            'failed_instances': 0,
            'total_duration': 0.0,
            'avg_duration': 0.0
        }
        self.logger.info(f"Added pipeline: {pipeline.name}")

    def execute_batch(self, pipeline_name: str, input_data_list: List[Dict], 
                     chunk_size: Optional[int] = None) -> List[Dict]:
        """
        Execute multiple instances of a pipeline with different inputs in parallel
        
        Args:
            pipeline_name: Name of the pipeline to execute
            input_data_list: List of input data dictionaries
            chunk_size: Optional; Number of items to process in each worker.
                       If not provided, will be set to len(input_data_list) / max_workers
            
        Returns:
            List of results dictionaries containing status and output for each instance
        """
        if pipeline_name not in self.pipelines:
            raise ValueError(f"Pipeline {pipeline_name} not found")

        pipeline = self.pipelines[pipeline_name]
        
        # Calculate optimal chunk size if not provided
        if chunk_size is None:
            chunk_size = max(1, len(input_data_list) // self.max_workers)
            self.logger.info(f"Auto-calculated chunk size: {chunk_size} "
                           f"(total items: {len(input_data_list)}, workers: {self.max_workers})")
        
        # Adjust number of workers based on chunk size and total items
        actual_workers = min(
            self.max_workers,
            (len(input_data_list) + chunk_size - 1) // chunk_size
        )
        
        try:
            pipeline_pickle = pickle.dumps(pipeline)
        except Exception as e:
            self.logger.error(f"Failed to pickle pipeline {pipeline_name}: {e}")
            raise

        # Group input data into chunks
        chunks = [
            input_data_list[i:i + chunk_size]
            for i in range(0, len(input_data_list), chunk_size)
        ]
        
        # Prepare worker arguments
        worker_args = [
            (pipeline_pickle, chunk_data, f"chunk_{i}")
            for i, chunk_data in enumerate(chunks)
        ]

        self.logger.info(f"Starting batch execution with {actual_workers} workers, "
                        f"{len(chunks)} chunks, chunk size {chunk_size}")

        results = []
        total_start_time = datetime.now()

        try:
            with Pool(processes=actual_workers) as pool:
                completed_chunks = 0
                
                for chunk_result in pool.imap_unordered(_pipeline_worker, worker_args):
                    results.extend(chunk_result)
                    completed_chunks += 1
                    
                    # Log progress by chunks
                    self.logger.info(
                        f"Progress: {completed_chunks}/{len(chunks)} chunks completed "
                        f"({len(results)}/{len(input_data_list)} total items)"
                    )
                    
                    self._update_metrics(pipeline_name, chunk_result)

        except Exception as e:
            self.logger.error(f"Batch execution failed: {e}")
            raise
        finally:
            self._finalize_metrics(pipeline_name, total_start_time)

        return results

    def get_status(self) -> Dict:
        """Get status and metrics for all pipelines"""
        return {
            "manager_name": self.name,
            "total_pipelines": len(self.pipelines),
            "execution_metrics": self.execution_metrics,
        }

    def _update_metrics(self, pipeline_name: str, chunk_result: List[Dict]) -> None:
        """Update metrics based on the results of a chunk"""
        for result in chunk_result:
            self.execution_metrics[pipeline_name]['total_instances'] += 1
            if result['success']:
                self.execution_metrics[pipeline_name]['successful_instances'] += 1
            else:
                self.execution_metrics[pipeline_name]['failed_instances'] += 1
                self.logger.error(
                    f"Pipeline instance {result['instance_id']} failed: {result['error']}"
                )

    def _finalize_metrics(self, pipeline_name: str, total_start_time: datetime) -> None:
        """Finalize metrics after batch execution"""
        total_duration = (datetime.now() - total_start_time).total_seconds()
        self.execution_metrics[pipeline_name]['total_duration'] += total_duration
        if self.execution_metrics[pipeline_name]['total_instances'] > 0:
            self.execution_metrics[pipeline_name]['avg_duration'] = (
                self.execution_metrics[pipeline_name]['total_duration'] /
                self.execution_metrics[pipeline_name]['total_instances']
            )