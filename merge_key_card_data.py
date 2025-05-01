#!/usr/bin/env python3
"""
Script to merge key card data from multiple sources.
Combines existing key card data with new data, avoiding duplicates.
"""
import argparse
import sys
import os
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("merge_key_card_data")

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from src.data_ingestion import merge_key_card_data

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Merge key card data files, removing duplicates."
    )
    
    parser.add_argument(
        "--existing", 
        type=str, 
        help="Path to existing key card data CSV file",
        default="data/raw/key_card_access.csv"
    )
    
    parser.add_argument(
        "--new", 
        type=str, 
        required=True,
        help="Path to new key card data CSV file to merge"
    )
    
    parser.add_argument(
        "--output", 
        type=str, 
        help="Path to save merged data (defaults to existing file path)"
    )
    
    parser.add_argument(
        "--no-backup", 
        action="store_true",
        help="Skip creating a backup of the existing file"
    )
    
    return parser.parse_args()

def main():
    """Main function to merge key card data."""
    args = parse_arguments()
    
    # Convert relative paths to absolute
    base_dir = Path(__file__).resolve().parent
    
    if not Path(args.existing).is_absolute():
        existing_path = base_dir / args.existing
    else:
        existing_path = Path(args.existing)
        
    if not Path(args.new).is_absolute():
        new_path = base_dir / args.new
    else:
        new_path = Path(args.new)
    
    # Set output path
    if args.output:
        if not Path(args.output).is_absolute():
            output_path = base_dir / args.output
        else:
            output_path = Path(args.output)
    else:
        output_path = existing_path
    
    # Log the paths
    logger.info(f"Existing data: {existing_path}")
    logger.info(f"New data: {new_path}")
    logger.info(f"Output path: {output_path}")
    
    # Create backup?
    create_backup = not args.no_backup
    if create_backup:
        logger.info("Will create backup of existing data")
    else:
        logger.warning("Skipping backup (--no-backup flag provided)")
    
    # Validate input paths
    if not existing_path.exists():
        logger.error(f"Existing file not found: {existing_path}")
        return 1
        
    if not new_path.exists():
        logger.error(f"New file not found: {new_path}")
        return 1
    
    # Validate output directory exists
    output_dir = output_path.parent
    if not output_dir.exists():
        logger.warning(f"Output directory doesn't exist: {output_dir}")
        try:
            output_dir.mkdir(parents=True)
            logger.info(f"Created output directory: {output_dir}")
        except Exception as e:
            logger.error(f"Failed to create output directory: {str(e)}")
            return 1
    
    # Merge the data
    try:
        merged_df = merge_key_card_data(
            str(existing_path),
            str(new_path),
            str(output_path),
            create_backup
        )
        
        if merged_df.empty:
            logger.error("Merging process failed")
            return 1
            
        logger.info(f"Success! Merged data saved to {output_path}")
        logger.info(f"Final dataset has {len(merged_df):,} rows")
        
        return 0
    
    except Exception as e:
        logger.error(f"Error during merge process: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())