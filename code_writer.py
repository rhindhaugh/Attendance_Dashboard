#!/usr/bin/env python3
"""
code_writer.py

Place this script in the root of your project. When run, it will generate a file
called 'full_codebase.txt' in the same folder. The output file contains:

1. A directory structure (folder/file hierarchy) for your code/data (excluding
   generic/system folders like .git, venv, node_modules, etc.).
2. The contents of each relevant file, prefaced by its path.
   - By default, we include .py, .csv, .txt, .md, .json, .yaml, .yml.
   - We skip other file types (feel free to adjust).
3. For CSV files, only the first 50 lines are included.
4. The result is a single text file you can upload to an LLM for context.

Usage:
    python code_writer.py
"""

import os

OUTPUT_FILENAME = "full_codebase.txt"
MAX_CSV_LINES = 50

# Directories to skip
EXCLUDED_DIRS = {
    ".git",
    "venv",
    "__pycache__",
    "node_modules",
    ".idea",
    ".vscode",
    ".cache"
}

# File extensions we want to include
ALLOWED_EXTENSIONS = {
    ".py",
    ".csv",
    ".txt",
    ".md",
    ".json",
    ".yaml",
    ".yml"
}

def main():
    project_root = os.path.abspath(".")
    with open(OUTPUT_FILENAME, "w", encoding="utf-8") as out_file:
        out_file.write("== START OF PROJECT CODEBASE DUMP ==\n")
        out_file.write(f"Project root: {project_root}\n\n")

        # 1) Write directory structure
        out_file.write("== DIRECTORY STRUCTURE (FILTERED) ==\n")
        for root, dirs, files in os.walk(project_root):
            # Filter out excluded directories
            dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS]

            # Calculate indentation based on depth
            rel_path = os.path.relpath(root, project_root)
            level = rel_path.count(os.sep)
            indent = "  " * level
            short_root = "." if rel_path == "." else f".{os.sep}{rel_path}"
            out_file.write(f"{indent}- {short_root}/\n")

            # Filter files by allowed extensions
            for f in sorted(files):
                ext = os.path.splitext(f)[1].lower()
                if ext in ALLOWED_EXTENSIONS:
                    sub_indent = "  " * (level + 1)
                    out_file.write(f"{sub_indent}- {f}\n")

        out_file.write("\n== FILE CONTENTS (FILTERED) ==\n")

        # 2) Write the contents of each allowed file
        for root, dirs, files in os.walk(project_root):
            # Filter out excluded directories
            dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS]

            for filename in sorted(files):
                ext = os.path.splitext(filename)[1].lower()
                if ext not in ALLOWED_EXTENSIONS:
                    continue  # Skip non-allowed file extensions

                file_path = os.path.join(root, filename)
                # Skip the output file itself
                if os.path.abspath(file_path) == os.path.abspath(os.path.join(project_root, OUTPUT_FILENAME)):
                    continue

                # Prepare relative path for display
                relative_path = os.path.relpath(file_path, project_root)

                out_file.write(f"\n==== START OF FILE: {relative_path} ====\n")

                # If it's a CSV, only read first 50 lines
                if ext == ".csv":
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            for i, line in enumerate(f):
                                if i >= MAX_CSV_LINES:
                                    out_file.write("... [Truncated after 50 lines]\n")
                                    break
                                out_file.write(line)
                    except Exception as e:
                        out_file.write(f"[Error reading CSV: {e}]\n")

                else:
                    # For allowed non-CSV files, read everything
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            contents = f.read()
                            out_file.write(contents)
                    except Exception as e:
                        out_file.write(f"[Error reading file: {e}]\n")

                out_file.write(f"\n==== END OF FILE: {relative_path} ====\n")

        out_file.write("\n== END OF PROJECT CODEBASE DUMP ==\n")

if __name__ == "__main__":
    main()
