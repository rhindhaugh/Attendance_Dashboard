#!/usr/bin/env python3
"""
Script to clean up backup files in the data/raw directory.
This script helps maintain a clean workspace by organizing and removing old backups.
"""
import os
import glob
import argparse
import shutil
from pathlib import Path
import datetime

def list_backup_files(data_dir):
    """List all backup files in the data directory."""
    backup_pattern = os.path.join(data_dir, "*.backup.*")
    backups = glob.glob(backup_pattern)
    
    print(f"Found {len(backups)} backup files:")
    for idx, backup in enumerate(backups, 1):
        backup_size = os.path.getsize(backup) / (1024 * 1024)  # Convert to MB
        print(f"{idx}. {os.path.basename(backup)} - {backup_size:.2f} MB")
    
    return backups

def clean_backup_files(data_dir, archive_dir=None, remove_all=False):
    """Clean up backup files by either archiving or removing them."""
    backups = list_backup_files(data_dir)
    
    if not backups:
        print("No backup files found.")
        return
    
    if remove_all:
        # Remove all backup files
        for backup in backups:
            try:
                os.remove(backup)
                print(f"Removed: {os.path.basename(backup)}")
            except Exception as e:
                print(f"Error removing {os.path.basename(backup)}: {e}")
        
        print(f"Removed {len(backups)} backup files.")
    
    elif archive_dir:
        # Create archive directory if it doesn't exist
        os.makedirs(archive_dir, exist_ok=True)
        
        # Create a timestamped subdirectory in the archive
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_subdir = os.path.join(archive_dir, f"backups_{timestamp}")
        os.makedirs(backup_subdir, exist_ok=True)
        
        # Move backup files to archive
        for backup in backups:
            try:
                dest = os.path.join(backup_subdir, os.path.basename(backup))
                shutil.move(backup, dest)
                print(f"Archived: {os.path.basename(backup)} -> {dest}")
            except Exception as e:
                print(f"Error archiving {os.path.basename(backup)}: {e}")
        
        print(f"Archived {len(backups)} backup files to {backup_subdir}")
    
    else:
        print("No action taken. Use --archive or --remove flag to specify an action.")

def main():
    """Main function to process command line arguments and clean up backups."""
    parser = argparse.ArgumentParser(description="Clean up backup files in the data directory")
    
    parser.add_argument(
        "--list", 
        action="store_true",
        help="List all backup files without taking any action"
    )
    
    parser.add_argument(
        "--archive", 
        action="store_true",
        help="Archive backup files to the archive directory"
    )
    
    parser.add_argument(
        "--remove", 
        action="store_true",
        help="Remove all backup files permanently"
    )
    
    args = parser.parse_args()
    
    # Set default paths
    base_dir = Path(__file__).resolve().parent
    data_dir = os.path.join(base_dir, "data", "raw")
    archive_dir = os.path.join(base_dir, "archive", "data_backups")
    
    # Just list files if requested
    if args.list or (not args.archive and not args.remove):
        list_backup_files(data_dir)
        return
    
    # Handle backup cleanup
    if args.remove:
        # Confirm before removing
        confirm = input("This will permanently remove all backup files. Continue? (y/n): ")
        if confirm.lower() == 'y':
            clean_backup_files(data_dir, remove_all=True)
        else:
            print("Operation cancelled.")
    elif args.archive:
        clean_backup_files(data_dir, archive_dir=archive_dir)

if __name__ == "__main__":
    main()