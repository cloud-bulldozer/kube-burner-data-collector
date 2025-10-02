#!/usr/bin/env python3
"""
Script to fetch and parse instances.json to create a distilled version containing only specific fields.
"""

import json
import sys
import os
import subprocess
import tempfile


def fetch_fresh_instances():
    """
    Fetch fresh instances.json from the source URL and return the data directly.
    All downloaded files are automatically cleaned up.
    
    Returns:
        list: Parsed instances data
    """
    url = "https://instances.vantagestaging.sh/www_pre_build.tar.gz"
    
    print(f"Fetching fresh instances data from: {url}")
    
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            print("Downloading and extracting data...")
            
            cmd = f"curl -L {url} | tar -xzf - -C {temp_dir}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                raise Exception(f"Failed to fetch data: {result.stderr}")

            instances_file = os.path.join(temp_dir, "www", "instances.json")
            
            if not os.path.exists(instances_file):
                raise Exception("instances.json not found at expected path: www/instances.json")
            
            print("Parsing instances data...")
            with open(instances_file, 'r', encoding='utf-8') as f:
                instances = json.load(f)
            
            print(f"Successfully fetched and parsed {len(instances)} instances")
            return instances
            
    except subprocess.CalledProcessError as e:
        raise Exception(f"Network error while fetching data: {e}")
    except Exception as e:
        raise Exception(f"Error fetching fresh instances data: {e}")


def parse_instances(instances_data, output_file):
    """
    Parse instances data and extract only the specified fields.
    
    Args:
        instances_data (list): List of instance dictionaries
        output_file (str): Path to the output distilled JSON file
    """
    # Fields to keep
    required_fields = [
        'instance_type',
        'vCPU',
        'physical_processor',
        'clock_speed_ghz',
        'memory',
        'network_performance'
    ]
    
    try:
        print(f"Processing {len(instances_data)} instances")
        
        distilled_instances = []
        
        for instance in instances_data:
            distilled_instance = {}
            
            for field in required_fields:
                if field in instance:
                    distilled_instance[field] = instance[field]
                else:
                    # Handle missing fields gracefully
                    distilled_instance[field] = None
            
            distilled_instances.append(distilled_instance)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(distilled_instances, f, indent=2, ensure_ascii=False)
        
        print(f"Successfully created distilled instances file: {output_file}")
        print(f"Extracted {len(distilled_instances)} instances with {len(required_fields)} fields each")
        
        print("\nSample of first 3 distilled instances:")
        for i, instance in enumerate(distilled_instances[:3]):
            print(f"\nInstance {i+1}:")
            for field, value in instance.items():
                print(f"  {field}: {value}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def main():
    """Main function to fetch fresh AWS EC2 instances data and create distilled output."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fetch fresh AWS EC2 instances data and create a distilled version with specific fields",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fetch_aws_ec2_metadata.py aws_ec2_instances.json
  python fetch_aws_ec2_metadata.py /tmp/aws_ec2_instances.json
        """
    )
    
    parser.add_argument(
        'output_file',
        help='Path to output distilled JSON file'
    )
    
    args = parser.parse_args()
    
    try:
        print("Fetching fresh instances data...")
        instances_data = fetch_fresh_instances()
        
        parse_instances(instances_data, args.output_file)
        
        print("All temporary files have been automatically cleaned up.")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
