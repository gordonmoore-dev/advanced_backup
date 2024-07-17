## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

# Advanced Backup Script

This Python script provides an advanced backup solution with support for incremental backups, batch processing, and various customization options.

## Prerequisites

- Python 3.6 or higher
- Required Python packages: 
  - `tqdm`
  - Other standard library packages (no additional installation required)

You can install the required package using pip:

```
pip install tqdm
```

## Basic Usage

```
python advanced_backup_gpt.py <source_directory> <destination_directory> [options]
```

### Arguments

- `source_directory`: The directory you want to backup
- `destination_directory`: The directory where the backup will be stored

### Options

- `--start-batch <number>`: Batch number to start from (default: 1)
- `--end-batch <number>`: Batch number to end at (optional)
- `--exclude <pattern1> <pattern2> ...`: Patterns to exclude (e.g., *.tmp)
- `--validate-local`: Run local validation
- `--validate-remote`: Run remote validation
- `--batch-size <size>`: Batch size in MB (default: 10)
- `--incremental`: Run an incremental backup
- `--resume`: Resume the most recent incomplete backup

## Examples

1. Perform a full backup:
   ```
   python advanced_backup_gpt.py /path/to/source /path/to/destination
   ```

2. Perform an incremental backup:
   ```
   python advanced_backup_gpt.py /path/to/source /path/to/destination --incremental
   ```

3. Exclude certain file types:
   ```
   python advanced_backup_gpt.py /path/to/source /path/to/destination --exclude *.tmp *.log
   ```

4. Resume the most recent incomplete backup:
   ```
   python advanced_backup_gpt.py /path/to/source /path/to/destination --resume
   ```

5. Validate a local backup:
   ```
   python advanced_backup_gpt.py /path/to/source /path/to/destination --validate-local
   ```

6. Run a backup with a specific batch size:
   ```
   python advanced_backup_gpt.py /path/to/source /path/to/destination --batch-size 50
   ```

## Notes

- The script creates a database file to track backup progress. This file is stored in the same directory as the script.
- Incremental backups only transfer files that have changed since the last backup.
- The `--validate-local` and `--validate-remote` options can be used to check the integrity of your backups.
- Use the `--resume` option to continue a backup that was interrupted.
