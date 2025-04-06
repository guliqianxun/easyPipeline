import logging
import time
import os
import random
import string
import numpy as np
import tempfile
import json
from typing import List, Dict, Any
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from execution.step import Step
from pipeline.pipeline import Pipeline
from manager.pipeline_manager import PipelineManager
from manager.task_scheduler import ExecutionMode

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("OutputTest")

# Custom JSON encoder to handle NumPy types
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)

# ===================================
# Math Operations Pipeline
# ===================================

class GenerateNumbersStep(Step):
    """Generate random numbers for testing"""
    def __init__(self, name="generate_numbers"):
        super().__init__(name)
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        count = data.get('count', 100)
        min_val = data.get('min_val', 1)
        max_val = data.get('max_val', 1000)
        
        # Convert to standard Python types immediately
        numbers = np.random.randint(min_val, max_val, count).tolist()
        logger.info(f"Generated {count} random numbers")
        
        # Update data state
        data['numbers'] = numbers
        data['source'] = 'random_generation'
        data['generation_time'] = time.time()
        
        # Simulate some work
        time.sleep(0.2)
        return data

class CalculateStatisticsStep(Step):
    """Calculate basic statistics on numbers"""
    def __init__(self, name="calculate_statistics"):
        super().__init__(name)
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        numbers = data.get('numbers', [])
        
        if not numbers:
            raise ValueError("No numbers provided for statistics calculation")
        
        # Calculate statistics and convert to Python types
        data['stats'] = {
            'mean': float(np.mean(numbers)),
            'median': float(np.median(numbers)),
            'std_dev': float(np.std(numbers)),
            'min': int(np.min(numbers)),
            'max': int(np.max(numbers)),
            'sum': int(np.sum(numbers)),
            'count': len(numbers)
        }
        
        logger.info(f"Calculated statistics: mean={data['stats']['mean']:.2f}, "
                  f"sum={data['stats']['sum']}")
        
        # Simulate work
        time.sleep(0.3)
        return data

class PerformOperationsStep(Step):
    """Perform mathematical operations on the numbers"""
    def __init__(self, name="perform_operations", operation="add"):
        super().__init__(name)
        self.operation = operation
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        numbers = data.get('numbers', [])
        
        if not numbers:
            raise ValueError("No numbers provided for operations")
            
        # Get operation parameters
        operand = data.get('operand', 10)
        
        # Perform the selected operation
        if self.operation == "add":
            result = [n + operand for n in numbers]
            operation_name = f"add_{operand}"
        elif self.operation == "multiply":
            result = [n * operand for n in numbers]
            operation_name = f"multiply_{operand}"
        elif self.operation == "divide":
            # Add error handling for division
            try:
                result = [n / operand for n in numbers]
                operation_name = f"divide_{operand}"
            except ZeroDivisionError:
                raise ValueError(f"Cannot divide by zero (operand={operand})")
        else:
            raise ValueError(f"Unknown operation: {self.operation}")
        
        # Store both the result and original data
        data[operation_name] = result
        
        # Randomly fail occasionally to test error handling
        if random.random() < 0.05:  # 5% chance to fail
            raise RuntimeError(f"Random failure in {self.operation} operation")
            
        logger.info(f"Performed {self.operation} operation with operand {operand}")
        time.sleep(0.2)
        return data

class SaveResultsStep(Step):
    """Save the results to a file"""
    def __init__(self, name="save_results"):
        super().__init__(name)
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        output_dir = data.get('output_dir', tempfile.gettempdir())
        filename = data.get('filename', f"math_results_{int(time.time())}.json")
        
        # Create directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save results to file
        filepath = os.path.join(output_dir, filename)
        
        # Use our custom encoder to handle NumPy types
        with open(filepath, 'w') as f:
            json.dump(data, f, cls=NumpyEncoder, indent=2)
        
        logger.info(f"Saved results to {filepath}")
        
        # Add filepath to data
        data['output_filepath'] = filepath
        
        return data

# ===================================
# File Processing Pipeline
# ===================================

class GenerateFilesStep(Step):
    """Generate random files for testing"""
    def __init__(self, name="generate_files"):
        super().__init__(name)
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        output_dir = data.get('output_dir', tempfile.gettempdir())
        file_count = data.get('file_count', 5)
        file_types = data.get('file_types', ['txt', 'csv', 'json'])
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate files
        generated_files = []
        
        for i in range(file_count):
            file_type = random.choice(file_types)
            file_path = os.path.join(output_dir, f"test_file_{i}.{file_type}")
            
            # Create file with different content based on type
            if file_type == 'txt':
                content = ''.join(random.choice(string.ascii_letters + string.digits) 
                                  for _ in range(100))
                with open(file_path, 'w') as f:
                    f.write(content)
            
            elif file_type == 'csv':
                # Create a simple CSV with random data
                lines = []
                headers = ['id', 'value', 'name']
                lines.append(','.join(headers))
                
                for j in range(5):
                    row = [
                        str(j),
                        str(random.randint(1, 100)),
                        ''.join(random.choice(string.ascii_letters) for _ in range(8))
                    ]
                    lines.append(','.join(row))
                
                with open(file_path, 'w') as f:
                    f.write('\n'.join(lines))
            
            elif file_type == 'json':
                # Create a JSON file with random data
                data_obj = {
                    'id': i,
                    'name': ''.join(random.choice(string.ascii_letters) for _ in range(8)),
                    'values': [random.randint(1, 100) for _ in range(5)],
                    'metadata': {
                        'created_at': time.time(),
                        'version': '1.0',
                        'tags': ['test', 'random', 'data']
                    }
                }
                
                with open(file_path, 'w') as f:
                    json.dump(data_obj, f, indent=2)
            
            # Add file info to list
            generated_files.append({
                'path': file_path,
                'type': file_type,
                'size': os.path.getsize(file_path)
            })
        
        logger.info(f"Generated {file_count} files in {output_dir}")
        
        # Update data with generated files
        data['files'] = generated_files
        
        return data

class ProcessFilesStep(Step):
    """Process each file based on its type"""
    def __init__(self, name="process_files"):
        super().__init__(name)
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        files = data.get('files', [])
        
        if not files:
            raise ValueError("No files to process")
        
        processed_files = []
        failed_files = []
        
        for file_info in files:
            file_path = file_info['path']
            file_type = file_info['type']
            
            logger.info(f"Processing file: {file_path}")
            
            try:
                # Process based on file type
                if file_type == 'txt':
                    with open(file_path, 'r') as f:
                        content = f.read()
                    
                    # Simple text processing
                    word_count = len(content.split())
                    char_count = len(content)
                    
                    file_info['processing_result'] = {
                        'word_count': word_count,
                        'char_count': char_count,
                        'stats': {
                            'avg_word_length': char_count / word_count if word_count > 0 else 0
                        }
                    }
                
                elif file_type == 'csv':
                    with open(file_path, 'r') as f:
                        lines = f.readlines()
                    
                    # Simple CSV processing
                    row_count = len(lines) - 1  # Excluding header
                    headers = lines[0].strip().split(',')
                    
                    file_info['processing_result'] = {
                        'headers': headers,
                        'row_count': row_count,
                        'column_count': len(headers)
                    }
                
                elif file_type == 'json':
                    with open(file_path, 'r') as f:
                        json_data = json.load(f)
                    
                    # Simple JSON processing
                    file_info['processing_result'] = {
                        'keys': list(json_data.keys()),
                        'structure': {
                            k: type(v).__name__ for k, v in json_data.items()
                        }
                    }
                
                # Randomly fail some files to test error handling
                if random.random() < 0.1:  # 10% chance to fail
                    raise RuntimeError(f"Random failure processing {file_type} file")
                
                # Add to processed files
                file_info['status'] = 'success'
                processed_files.append(file_info)
                
            except Exception as e:
                # Add to failed files
                file_info['status'] = 'failed'
                file_info['error'] = str(e)
                failed_files.append(file_info)
                logger.error(f"Failed to process {file_path}: {e}")
            
            # Simulate processing work
            time.sleep(0.3)
        
        # Update data with processed results
        data['processed_files'] = processed_files
        data['failed_files'] = failed_files
        data['processing_stats'] = {
            'total': len(files),
            'successful': len(processed_files),
            'failed': len(failed_files)
        }
        
        logger.info(f"Processed {len(processed_files)} files successfully, {len(failed_files)} failed")
        
        return data

class GenerateReportStep(Step):
    """Generate a report summarizing file processing results"""
    def __init__(self, name="generate_report"):
        super().__init__(name)
    
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        output_dir = data.get('output_dir', tempfile.gettempdir())
        report_file = os.path.join(output_dir, f"file_processing_report_{int(time.time())}.json")
        
        # Create directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Extract processing statistics
        processing_stats = data.get('processing_stats', {})
        processed_files = data.get('processed_files', [])
        failed_files = data.get('failed_files', [])
        
        # Generate report structure
        report = {
            'timestamp': time.time(),
            'summary': processing_stats,
            'successful_files': [{
                'path': f['path'],
                'type': f['type'],
                'result': f.get('processing_result', {})
            } for f in processed_files],
            'failed_files': [{
                'path': f['path'],
                'type': f['type'],
                'error': f.get('error', 'Unknown error')
            } for f in failed_files]
        }
        
        # Group files by type
        file_types = {}
        for f in processed_files:
            file_type = f['type']
            if file_type not in file_types:
                file_types[file_type] = {'count': 0, 'files': []}
            file_types[file_type]['count'] += 1
            file_types[file_type]['files'].append(f['path'])
        
        report['file_types'] = file_types
        
        # Save report to file
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Generated report at {report_file}")
        
        # Add report info to data
        data['report_file'] = report_file
        data['report_summary'] = {
            'file_type': list(file_types.keys()),
            'total_files': len(processed_files) + len(failed_files),
            'successful': len(processed_files),
            'failed': len(failed_files)
        }
        
        return data

# ===================================
# Pipeline Creation Functions
# ===================================

def create_math_pipeline() -> Pipeline:
    """Create a pipeline for math operations"""
    pipeline = Pipeline(name="math_operations", description="Perform math operations on random numbers")
    
    # Create and add steps
    gen_step = GenerateNumbersStep()
    stats_step = CalculateStatisticsStep()
    add_step = PerformOperationsStep(name="add_operation", operation="add")
    multiply_step = PerformOperationsStep(name="multiply_operation", operation="multiply")
    divide_step = PerformOperationsStep(name="divide_operation", operation="divide")
    save_step = SaveResultsStep()
    
    # Add steps to pipeline with dependencies
    pipeline.add_step(gen_step)
    pipeline.add_step(stats_step, depends_on=[gen_step.name])
    pipeline.add_step(add_step, depends_on=[stats_step.name])
    pipeline.add_step(multiply_step, depends_on=[stats_step.name])
    pipeline.add_step(divide_step, depends_on=[stats_step.name])
    pipeline.add_step(save_step, depends_on=[add_step.name, multiply_step.name, divide_step.name])
    
    return pipeline

def create_file_pipeline() -> Pipeline:
    """Create a pipeline for file generation and processing"""
    pipeline = Pipeline(name="file_processor", description="Generate and process files")
    
    # Create and add steps
    gen_files_step = GenerateFilesStep()
    process_files_step = ProcessFilesStep()
    report_step = GenerateReportStep()
    
    # Add steps to pipeline with dependencies
    pipeline.add_step(gen_files_step)
    pipeline.add_step(process_files_step, depends_on=[gen_files_step.name])
    pipeline.add_step(report_step, depends_on=[process_files_step.name])
    
    return pipeline

# ===================================
# Test Functions
# ===================================

def test_math_operations():
    """Test math operations pipeline in parallel thread mode"""
    logger.info("\n===== TESTING MATH OPERATIONS PIPELINE =====")
    
    # Create pipeline manager with parallel thread execution
    manager = PipelineManager(
        name="math_manager",
        execution_mode=ExecutionMode.PARALLEL_THREAD,
        max_workers=4
    )
    
    # Create and register pipeline
    math_pipeline = create_math_pipeline()
    manager.register_pipeline(math_pipeline)
    
    # Create test data
    data_list = [
        {
            'count': 100,
            'min_val': 1,
            'max_val': 1000,
            'operand': 5,
            'output_dir': os.path.join(tempfile.gettempdir(), 'math_test'),
            'filename': f"math_results_{i}.json"
        }
        for i in range(5)
    ]
    
    # Execute pipeline
    logger.info(f"Executing math operations pipeline with {len(data_list)} tasks")
    group_id = manager.execute({
        "math_operations": data_list
    })
    
    # Track progress
    completed = False
    while not completed:
        group_status = manager.get_group_status(group_id)
        if group_status:
            logger.info(f"Progress: {group_status['completed_tasks']}/{group_status['total_tasks']} "
                      f"(Success: {group_status['successful_tasks']}, Failed: {group_status['failed_tasks']})")
            
            completed = group_status['is_complete']
        
        time.sleep(1)
    
    # Get results
    results = manager.get_result("math_operations")
    failures = manager.get_failures("math_operations")
    
    logger.info(f"Execution complete. Successful tasks: {len(results)}, Failed tasks: {len(failures)}")
    
    # Print output file paths
    for result in results:
        if 'output_filepath' in result:
            logger.info(f"Output file: {result['output_filepath']}")
    
    # Shutdown the manager
    manager.shutdown()

def test_file_processing():
    """Test file processing pipeline in parallel process mode"""
    logger.info("\n===== TESTING FILE PROCESSING PIPELINE =====")
    
    # Create pipeline manager with parallel process execution
    manager = PipelineManager(
        name="file_manager",
        execution_mode=ExecutionMode.PARALLEL_PROCESS,
        max_workers=3
    )
    
    # Create and register pipeline
    file_pipeline = create_file_pipeline()
    manager.register_pipeline(file_pipeline)
    
    # Create test data
    data_list = [
        {
            'output_dir': os.path.join(tempfile.gettempdir(), f'file_test_{i}'),
            'file_count': random.randint(3, 8),
            'file_types': ['txt', 'csv', 'json']
        }
        for i in range(4)
    ]
    
    # Execute pipeline
    logger.info(f"Executing file processing pipeline with {len(data_list)} tasks")
    group_id = manager.execute({
        "file_processor": data_list
    })
    
    # Track progress
    completed = False
    while not completed:
        group_status = manager.get_group_status(group_id)
        if group_status:
            logger.info(f"Group progress: {group_status['completed_tasks']}/{group_status['total_tasks']} "
                      f"(Success: {group_status['successful_tasks']}, Failed: {group_status['failed_tasks']})")
            
            completed = group_status['is_complete']
        
        time.sleep(1)
    
    # Get final status
    status = manager.get_status("file_processor")
    logger.info(f"Final status: Total={status['total']}, Success={status['success']}, "
              f"Failed={status['failed']}, In Progress={status['in_progress']}")
    
    # Get results 
    results = manager.get_result("file_processor")
    logger.info(f"Received {len(results)} results")
    
    # Print report file paths
    logger.info("Generated report files:")
    for result in results:
        if 'report_file' in result:
            logger.info(f"  - {result['report_file']}")
            
            # Print a summary for each report
            if 'report_summary' in result:
                summary = result['report_summary']
                logger.info(f"    Summary: {summary['file_type']} files - "
                          f"Total: {summary['total_files']}, "
                          f"Success: {summary['successful']}, "
                          f"Failed: {summary['failed']}")
    
    # Shutdown the manager
    manager.shutdown()

def test_batch_execution():
    """Test batch execution of a large number of tasks"""
    logger.info("\n===== TESTING BATCH EXECUTION =====")
    
    # Create pipeline manager with batch execution
    manager = PipelineManager(
        name="batch_manager",
        execution_mode=ExecutionMode.BATCH,
        max_workers=4
    )
    
    # Create and register pipeline
    math_pipeline = create_math_pipeline()
    manager.register_pipeline(math_pipeline)
    
    # Create a large batch of test data
    data_list = [
        {
            'count': random.randint(50, 200),
            'min_val': random.randint(1, 100),
            'max_val': random.randint(500, 2000),
            'operand': random.randint(2, 20),
            'output_dir': os.path.join(tempfile.gettempdir(), 'batch_test'),
            'filename': f"batch_result_{i}.json"
        }
        for i in range(20)  # 20 tasks
    ]
    
    # Execute pipeline in batch mode
    logger.info(f"Starting batch execution with {len(data_list)} tasks")
    group_id = manager.execute({
        "math_operations": data_list
    })
    
    # Track execution status with progress percentage
    logger.info("Monitoring batch execution progress...")
    
    start_time = time.time()
    last_status = {}
    
    while True:
        status = manager.get_status("math_operations")
        
        # Only log when status changes
        if status != last_status:
            completed = status['success'] + status['failed']
            total = status['total']
            progress_pct = (completed / total * 100) if total > 0 else 0
            elapsed = time.time() - start_time
            
            logger.info(f"Progress: {completed}/{total} ({progress_pct:.1f}%) in {elapsed:.1f}s - "
                       f"Success: {status['success']}, Failed: {status['failed']}")
            
            last_status = status.copy()
        
        if status['in_progress'] == 0:
            break
            
        time.sleep(1)
    
    # Get stats on execution time
    elapsed_time = time.time() - start_time
    logger.info(f"Batch execution completed in {elapsed_time:.2f} seconds")
    logger.info(f"Average time per task: {elapsed_time / len(data_list):.2f} seconds")
    
    # Get success and failure counts
    results = manager.get_result("math_operations")
    failures = manager.get_failures("math_operations")
    
    logger.info(f"Successful tasks: {len(results)}")
    logger.info(f"Failed tasks: {len(failures)}")
    
    # Shutdown the manager
    manager.shutdown()
    
# ===================================
# Main Execution 
# ===================================

if __name__ == "__main__":
    logger.info("Starting easyPipeline output tests")
    
    # Run the math operations test
    test_math_operations()
    
    # Run the file processing test
    test_file_processing()
    
    # Run the batch execution test
    test_batch_execution()
    
    logger.info("All tests completed!")