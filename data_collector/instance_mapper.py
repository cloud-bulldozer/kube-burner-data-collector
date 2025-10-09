"""
Functionality for mapping AWS EC2 instance types to specifications.
"""

import json
import logging
from typing import Dict, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class InstanceMapper:
    """Maps AWS EC2 instance types to their specifications."""
    
    def __init__(self, aws_instances_file_path: str = None):
        """
        Initialize the instance mapper with AWS EC2 instances data.
        
        Args:
            aws_instances_file: Path to the AWS EC2 instances JSON file
        """
        self.aws_instances_file_path = aws_instances_file_path
        self.instance_specs = {}
        self._load_aws_instances()
    
    def _load_aws_instances(self) -> None:
        """Load AWS EC2 instances data from JSON file."""
        try:
            instances_path = Path(self.aws_instances_file_path)
            
            if not instances_path.exists():
                raise FileNotFoundError(f"AWS instances file not found: {instances_path}")
            
            with open(instances_path, 'r', encoding='utf-8') as f:
                instances_data = json.load(f)
            
            for instance in instances_data:
                instance_type = instance.get('instance_type')
                if instance_type:
                    self.instance_specs[instance_type] = {
                        'vCPU': instance.get('vCPU'),
                        'physical_processor': instance.get('physical_processor'),
                        'clock_speed_ghz': instance.get('clock_speed_ghz'),
                        'memory': instance.get('memory'),
                        'network_performance': instance.get('network_performance')
                    }
            
            logger.info(f"Loaded {len(self.instance_specs)} AWS instance specifications")
            
        except Exception as e:
            logger.error(f"Failed to load AWS instances data: {e}")
            raise
    
    def get_instance_specs(self, instance_type: str) -> Optional[Dict[str, Any]]:
        """
        Get specifications for a given instance type.
        
        Args:
            instance_type: AWS instance type (e.g., 'm6a.xlarge')
            
        Returns:
            Dictionary containing instance specifications
        """
        return self.instance_specs.get(instance_type)
    
    
    def map_instance_types_from_metadata(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map instance types from metadata.
        
        Args:
            metadata: Metadata containing instance type information
            
        Returns:
            Dictionary with mapped instance specifications
        """
        mapped_data = {}
        
        node_types = [
            ('masterNodesType', 'masterNode'),
            ('workerNodesType', 'workerNode'),
            ('infraNodesType', 'infraNode')
        ]
        
        for node_type_key, prefix in node_types:
            instance_type = metadata.get(node_type_key)
            if instance_type:
                specs = self.get_instance_specs(instance_type)
                if specs:
                    mapped_data[f'{prefix}vCPU'] = specs.get('vCPU')
                    mapped_data[f'{prefix}PhysicalProcessor'] = specs.get('physical_processor')
                    mapped_data[f'{prefix}ClockSpeedGhz'] = specs.get('clock_speed_ghz')
                    mapped_data[f'{prefix}Memory'] = specs.get('memory')
                    mapped_data[f'{prefix}NetworkPerformance'] = specs.get('network_performance')
                else:
                    logger.warning(f"{prefix} instance type '{instance_type}' not found in AWS instances data")
                    mapped_data[f'{prefix}vCPU'] = None
                    mapped_data[f'{prefix}PhysicalProcessor'] = None
                    mapped_data[f'{prefix}ClockSpeedGhz'] = None
                    mapped_data[f'{prefix}Memory'] = None
                    mapped_data[f'{prefix}NetworkPerformance'] = None
        
        return mapped_data
