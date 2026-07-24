import numpy as np
import statistics

# Instance type specifications
STANDARD_INSTANCES = {
    'db.t4g.medium': {'cpu': 2, 'memory': 4},
    'db.r6g.large': {'cpu': 2, 'memory': 16},
    'db.r6g.xlarge': {'cpu': 4, 'memory': 32},
    'db.r6g.2xlarge': {'cpu': 8, 'memory': 64},
    'db.r6g.4xlarge': {'cpu': 16, 'memory': 128},
    'db.r6g.8xlarge': {'cpu': 32, 'memory': 256},
    'db.r6g.12xlarge': {'cpu': 48, 'memory': 384},
    'db.r6g.16xlarge': {'cpu': 64, 'memory': 512},
    'db.r8g.large': {'cpu': 2, 'memory': 16},
    'db.r8g.xlarge': {'cpu': 4, 'memory': 32},
    'db.r8g.2xlarge': {'cpu': 8, 'memory': 64},
    'db.r8g.4xlarge': {'cpu': 16, 'memory': 128},
    'db.r8g.8xlarge': {'cpu': 32, 'memory': 256},
    'db.r8g.12xlarge': {'cpu': 48, 'memory': 384},
    'db.r8g.16xlarge': {'cpu': 64, 'memory': 512},
    'db.r5.24xlarge': {'cpu': 96, 'memory': 768}
}

NVME_INSTANCES = {
    'db.r6gd.xlarge': {'cpu': 4, 'memory': 32},
    'db.r6gd.2xlarge': {'cpu': 8, 'memory': 64},
    'db.r6gd.4xlarge': {'cpu': 16, 'memory': 128},
    'db.r6gd.8xlarge': {'cpu': 32, 'memory': 256},
    'db.r6gd.12xlarge': {'cpu': 48, 'memory': 384},
    'db.r6gd.16xlarge': {'cpu': 64, 'memory': 512}
}

def analyze_workload_statistics(cpu_datapoints, buffer_cache_datapoints=None):
    """Analyze workload patterns using statistical methods"""
    if not cpu_datapoints or len(cpu_datapoints) < 10:
        return {'pattern': 'insufficient_data', 'confidence': 0, 'stats': {}}
    
    # Filter out None values
    cpu_datapoints = [x for x in cpu_datapoints if x is not None]
    if len(cpu_datapoints) < 10:
        return {'pattern': 'insufficient_data', 'confidence': 0, 'stats': {}}
    
    cpu_array = np.array(cpu_datapoints)
    
    # Calculate statistical metrics
    cpu_mean = np.mean(cpu_array)
    cpu_std = np.std(cpu_array)
    cpu_cv = (cpu_std / cpu_mean * 100) if cpu_mean > 0 else 0
    cpu_min = np.min(cpu_array)
    cpu_max = np.max(cpu_array)
    cpu_p95 = np.percentile(cpu_array, 95)
    cpu_p5 = np.percentile(cpu_array, 5)
    
    # Calculate spike frequency (values > mean + 2*std)
    spike_threshold = cpu_mean + (2 * cpu_std)
    spikes = cpu_array[cpu_array > spike_threshold]
    spike_frequency = len(spikes) / len(cpu_array) * 100
    
    # Calculate idle frequency (values < 20%)
    idle_threshold = 20
    idle_periods = cpu_array[cpu_array < idle_threshold]
    idle_frequency = len(idle_periods) / len(cpu_array) * 100
    
    # Determine workload pattern
    pattern = 'unknown'
    confidence = 0
    
    # Spiky workload detection
    if cpu_cv > 50 and spike_frequency > 10 and idle_frequency > 30:
        pattern = 'highly_spiky'
        confidence = min(95, 60 + (cpu_cv - 50) * 0.7 + spike_frequency * 0.5)
    elif cpu_cv > 30 and (spike_frequency > 5 or idle_frequency > 20):
        pattern = 'moderately_spiky'
        confidence = min(85, 50 + (cpu_cv - 30) * 1.0 + spike_frequency * 0.8)
    elif cpu_cv < 20 and cpu_mean > 15 and spike_frequency < 5:
        pattern = 'sustained'
        confidence = min(90, 70 + (20 - cpu_cv) * 1.0)
    else:
        pattern = 'mixed'
        confidence = 40
    
    stats = {
        'cpu_mean': cpu_mean,
        'cpu_std': cpu_std,
        'cpu_cv': cpu_cv,
        'cpu_min': cpu_min,
        'cpu_max': cpu_max,
        'cpu_p95': cpu_p95,
        'cpu_p5': cpu_p5,
        'spike_frequency': spike_frequency,
        'idle_frequency': idle_frequency,
        'spike_threshold': spike_threshold
    }
    
    return {
        'pattern': pattern,
        'confidence': confidence,
        'stats': stats
    }

def get_current_instance_specs(current_instance_type):
    """Get current instance specifications"""
    if current_instance_type in STANDARD_INSTANCES:
        return STANDARD_INSTANCES[current_instance_type], 'standard'
    elif current_instance_type in NVME_INSTANCES:
        return NVME_INSTANCES[current_instance_type], 'nvme'
    else:
        return None, 'unknown'

def calculate_utilization_efficiency(stats, current_specs):
    """Calculate how efficiently current instance is being used"""
    cpu_efficiency = stats['cpu_mean'] / 100  # Convert percentage to ratio
    
    # Memory efficiency estimation (simplified)
    # Assume memory usage correlates with CPU for estimation
    estimated_memory_usage = min(100, stats['cpu_mean'] * 1.2)  # Rough estimation
    memory_efficiency = estimated_memory_usage / 100
    
    return {
        'cpu_efficiency': cpu_efficiency,
        'memory_efficiency': memory_efficiency,
        'overall_efficiency': (cpu_efficiency + memory_efficiency) / 2
    }

def recommend_instance_type(current_instance_type, workload_analysis, buffer_cache_hit_ratio=None):
    """Generate data-driven instance type recommendation"""
    
    current_specs, current_type = get_current_instance_specs(current_instance_type)
    if not current_specs:
        return {
            'recommendation': 'unknown_instance',
            'reason': f'Unknown current instance type: {current_instance_type}',
            'confidence': 0
        }
    
    stats = workload_analysis['stats']
    pattern = workload_analysis['pattern']
    confidence = workload_analysis['confidence']
    
    efficiency = calculate_utilization_efficiency(stats, current_specs)
    
    # Decision logic based on statistical analysis
    recommendation = None
    reason = ""
    
    # Serverless recommendation (highest priority for spiky workloads)
    if pattern in ['highly_spiky', 'moderately_spiky']:
        if stats['idle_frequency'] > 25 and stats['spike_frequency'] > 8:
            dcu_estimate = max(0.5, (stats['cpu_p95'] / 100) * (current_specs['cpu'] / 4))  # 1 DCU = 4 CPU
            recommendation = {
                'type': 'serverless',
                'instance': f'DocumentDB Serverless ({dcu_estimate:.1f} DCU starting capacity)',
                'specs': f'Auto-scaling from 0.5 to {dcu_estimate * 2:.1f} DCU'
            }
            reason = f"Highly variable workload detected: {stats['cpu_cv']:.1f}% coefficient of variation, {stats['spike_frequency']:.1f}% spike frequency, {stats['idle_frequency']:.1f}% idle time. Serverless will optimize costs during low usage periods."
    
    # NVMe recommendation (I/O bottleneck detection)
    elif current_type == 'standard' and buffer_cache_hit_ratio and buffer_cache_hit_ratio < 85:
        if stats['cpu_mean'] < 60:  # Not CPU bound
            # Find equivalent NVMe instance
            for nvme_instance, nvme_specs in NVME_INSTANCES.items():
                if nvme_specs['cpu'] == current_specs['cpu'] and nvme_specs['memory'] == current_specs['memory']:
                    recommendation = {
                        'type': 'nvme',
                        'instance': nvme_instance,
                        'specs': f"{nvme_specs['cpu']} vCPU, {nvme_specs['memory']} GB RAM + NVMe"
                    }
                    reason = f"I/O bottleneck detected: Buffer cache hit ratio {buffer_cache_hit_ratio:.1f}% (target >90%), CPU utilization only {stats['cpu_mean']:.1f}%. NVMe storage will improve I/O performance."
                    break
    
    # Downgrade recommendation (over-provisioned)
    elif efficiency['overall_efficiency'] < 0.3 and stats['cpu_p95'] < 50:
        # Find smaller standard instance (never recommend t3/t4g — burstable types not suitable for production)
        target_cpu = max(2, int(current_specs['cpu'] / 2))
        for instance, specs in STANDARD_INSTANCES.items():
            if instance.startswith('db.t'):
                continue  # Skip burstable instance types
            if specs['cpu'] == target_cpu:
                recommendation = {
                    'type': 'standard_downgrade',
                    'instance': instance,
                    'specs': f"{specs['cpu']} vCPU, {specs['memory']} GB RAM"
                }
                reason = f"Over-provisioned instance: Average CPU {stats['cpu_mean']:.1f}%, P95 CPU {stats['cpu_p95']:.1f}%, efficiency {efficiency['overall_efficiency']*100:.1f}%. Smaller instance will reduce costs while maintaining performance."
                break
    
    # Upgrade recommendation (under-provisioned)
    elif stats['cpu_p95'] > 80 and stats['cpu_mean'] > 60:
        # Find larger standard instance (never recommend t3/t4g)
        target_cpu = current_specs['cpu'] * 2
        for instance, specs in STANDARD_INSTANCES.items():
            if instance.startswith('db.t'):
                continue  # Skip burstable instance types
            if specs['cpu'] == target_cpu:
                recommendation = {
                    'type': 'standard_upgrade',
                    'instance': instance,
                    'specs': f"{specs['cpu']} vCPU, {specs['memory']} GB RAM"
                }
                reason = f"Under-provisioned instance: P95 CPU {stats['cpu_p95']:.1f}%, average CPU {stats['cpu_mean']:.1f}%. Larger instance needed to handle peak loads effectively."
                break
    
    # No change recommendation
    else:
        recommendation = {
            'type': 'no_change',
            'instance': current_instance_type,
            'specs': f"{current_specs['cpu']} vCPU, {current_specs['memory']} GB RAM"
        }
        reason = f"Current instance is appropriately sized: Average CPU {stats['cpu_mean']:.1f}%, P95 CPU {stats['cpu_p95']:.1f}%, workload pattern is {pattern}."
    
    return {
        'recommendation': recommendation,
        'reason': reason,
        'confidence': confidence,
        'current_efficiency': efficiency,
        'workload_stats': stats
    }