import os
import sqlite3
import hashlib
from tqdm import tqdm
import shutil
import argparse
import fnmatch
import time
import logging
import sys
import stat
import json
#from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Optional, Tuple, Union, Dict, Any

def adapt_datetime(dt):
    return dt.isoformat()

def convert_datetime(s):
    return datetime.fromisoformat(s)

# Register the adapter and converter
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("datetime", convert_datetime)


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_db_connection(db_file):
    return sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES)

def create_db(conn: sqlite3.Connection) -> None:
    c = conn.cursor()
    c.execute('''
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY,
        path TEXT,
        checksum TEXT,
        last_modified REAL,
        batch_number INTEGER,
        batch_complete INTEGER DEFAULT 0,
        is_directory INTEGER,
        run_number INTEGER,
        status TEXT
    )
    ''')
    c.execute('''
    CREATE TABLE IF NOT EXISTS backup_runs (
        id INTEGER PRIMARY KEY,
        run_number INTEGER UNIQUE,
        start_time datetime,
        end_time datetime,
        type TEXT,
        exclude_patterns TEXT
    )
    ''')
    conn.commit()

def start_new_run(conn: sqlite3.Connection, run_type: str, exclude_patterns: List[str]) -> int:
    run_number = get_latest_run_number(conn) + 1
    c = conn.cursor()
    c.execute("INSERT INTO backup_runs (run_number, start_time, type, exclude_patterns) VALUES (?, ?, ?, ?)",
              (run_number, datetime.now(), run_type, json.dumps(exclude_patterns)))
    conn.commit()
    return run_number

def get_exclude_patterns(conn: sqlite3.Connection, run_number: int) -> List[str]:
    c = conn.cursor()
    c.execute("SELECT exclude_patterns FROM backup_runs WHERE run_number = ?", (run_number,))
    result = c.fetchone()
    return json.loads(result[0]) if result else []


def end_run(conn: sqlite3.Connection, run_number: int) -> None:
    c = conn.cursor()
    c.execute("UPDATE backup_runs SET end_time = ? WHERE run_number = ?",
              (datetime.now(), run_number))
    conn.commit()

def insert_item(conn: sqlite3.Connection, path: str, checksum: Optional[str], last_modified: float, batch_number: int, is_directory: int, run_number: int, status: str) -> None:
    c = conn.cursor()
    c.execute('''
    INSERT OR REPLACE INTO items 
    (path, checksum, last_modified, batch_number, batch_complete, is_directory, run_number, status)
    VALUES (?, ?, ?, ?, 0, ?, ?, ?)
    ''', (path, checksum, last_modified, batch_number, is_directory, run_number, status))
    conn.commit()

def get_items_by_batch(conn: sqlite3.Connection, batch_number: int, run_number: int) -> List[Tuple[Any, ...]]:
    c = conn.cursor()
    c.execute('SELECT * FROM items WHERE batch_number=? AND run_number=?', (batch_number, run_number))
    return c.fetchall()

def mark_batch_complete(conn: sqlite3.Connection, batch_number: int, run_number: int) -> None:
    c = conn.cursor()
    c.execute('UPDATE items SET batch_complete=1 WHERE batch_number=? AND run_number=?', (batch_number, run_number))
    conn.commit()

def get_incomplete_batches(conn: sqlite3.Connection, start_batch: int, end_batch: Optional[int], run_number: int) -> List[Tuple[int]]:
    c = conn.cursor()
    if end_batch is not None:
        c.execute('SELECT DISTINCT batch_number FROM items WHERE batch_complete=0 AND batch_number BETWEEN ? AND ? AND run_number=? ORDER BY batch_number', (start_batch, end_batch, run_number))
    else:
        c.execute('SELECT DISTINCT batch_number FROM items WHERE batch_complete=0 AND batch_number >= ? AND run_number=? ORDER BY batch_number', (start_batch, run_number))
    return c.fetchall()

def get_file_info(path: str) -> Tuple[float, int]:
    last_modified = os.path.getmtime(path)
    file_size = os.path.getsize(path)
    return last_modified, file_size

def get_batch_info(conn: sqlite3.Connection, batch_number: int, run_number: int, source_dir: str) -> Tuple[int, int, int]:
    c = conn.cursor()
    c.execute('''
    SELECT path, is_directory FROM items 
    WHERE batch_number = ? AND run_number = ? AND status != 'deleted'
    ''', (batch_number, run_number))
    items = c.fetchall()
    
    total_size = 0
    file_count = 0
    for path, is_directory in items:
        if not is_directory:
            try:
                total_size += os.path.getsize(os.path.join(source_dir, path))
                file_count += 1
            except OSError as e:
                logging.warning(f"Couldn't get size of {path}: {e}")
    
    return len(items), file_count, total_size

def get_adaptive_chunk_size(file_size: int) -> int:
    """
    Determine an appropriate chunk size based on file size.
    
    For files:
    - Under 1 MB: use 256 KB chunks
    - 1 MB to 1 GB: use 50 MB chunks
    - Over 1 GB: use 200 MB chunks
    """
    if file_size < 1024 * 1024:  # Under 1 MB
        return 256 * 1024  # 256 KB
    elif file_size < 1024 * 1024 * 1024:  # Under 1 GB
        return 50 * 1024 * 1024  # 50 MB
    else:  # 1 GB and over
        return 200 * 1024 * 1024  # 200 MB

def calculate_checksum(path: str, chunk_size: Optional[int] = None) -> str:
    """Calculate file checksum in chunks to handle large files."""
    file_size = os.path.getsize(path)
    chunk_size = chunk_size or get_adaptive_chunk_size(file_size)
    
    hasher = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            hasher.update(chunk)
    return hasher.hexdigest()

def copy_large_file(src: str, dst: str, chunk_size: Optional[int] = None) -> None:
    """Copy large file in chunks."""
    file_size = os.path.getsize(src)
    chunk_size = chunk_size or get_adaptive_chunk_size(file_size)
    
    with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
        shutil.copyfileobj(fsrc, fdst, length=chunk_size)

def copy_directory(src: str, dst: str) -> None:
    os.makedirs(dst, exist_ok=True)
    shutil.copystat(src, dst)

def should_exclude(path: str, exclude_patterns: List[str]) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in exclude_patterns)

def scan_and_record_items(conn: sqlite3.Connection, source_dir: str, batch_size: int = 100 * 1024 * 1024, exclude_patterns: List[str] = [], run_number: int = 1, incremental: bool = False) -> None:
    total_size = 0
    batch_number = 1
    items_processed = 0
    
    cursor = conn.cursor()
    
    insert_sql = '''
    INSERT OR REPLACE INTO items 
    (path, checksum, last_modified, batch_number, batch_complete, is_directory, run_number, status)
    VALUES (?, ?, ?, ?, 0, ?, ?, ?)
    '''
    
    exclude_set = set(exclude_patterns)
    
    def should_exclude(path: str) -> bool:
        return any(fnmatch.fnmatch(path, pattern) for pattern in exclude_set)
    
    def insert_item(path: str, checksum: Optional[str], last_modified: float, batch_number: int, is_directory: int, status: str) -> None:
        nonlocal items_processed
        cursor.execute(insert_sql, (path, checksum, last_modified, batch_number, is_directory, run_number, status))
        items_processed += 1
        if items_processed % 10000 == 0:
            conn.commit()
    
    existing_items: Dict[str, Tuple[float, str]] = {}
    if incremental:
        cursor.execute("SELECT MAX(run_number) FROM backup_runs WHERE run_number < ?", (run_number,))
        last_run_number = cursor.fetchone()[0]
        if last_run_number is None:
            logging.warning("No previous backup found. Performing a full backup.")
            incremental = False
        else:
            cursor.execute("SELECT path, last_modified, checksum FROM items WHERE run_number = ?", (last_run_number,))
            existing_items = {path: (last_modified, checksum) for path, last_modified, checksum in cursor.fetchall()}
    
    with tqdm(desc="Scanning and recording items", unit=" items") as pbar:
        for root, dirs, files in os.walk(source_dir, topdown=True):
            dirs[:] = [d for d in dirs if not should_exclude(os.path.join(root, d))]
            
            for dir_name in dirs:
                dir_path = os.path.normpath(os.path.join(root, dir_name))
                last_modified = os.path.getmtime(dir_path)
                status = 'added'
                if incremental and dir_path in existing_items:
                    if abs(last_modified - existing_items[dir_path][0]) > 1:
                        status = 'updated'
                    else:
                        status = 'unchanged'
                insert_item(dir_path, None, last_modified, batch_number, 1, status)
                pbar.update(1)
            
            for file in files:
                file_path = os.path.normpath(os.path.join(root, file))
                if not should_exclude(file_path):
                    try:
                        last_modified = os.path.getmtime(file_path)
                        file_size = os.path.getsize(file_path)
                        
                        status = 'added'
                        checksum = None
                        if incremental and file_path in existing_items:
                            existing_last_modified, existing_checksum = existing_items[file_path]
                            if abs(last_modified - existing_last_modified) > 1:
                                status = 'updated'
                            else:
                                status = 'unchanged'
                                checksum = existing_checksum
                        
                        if total_size + file_size > batch_size:
                            batch_number += 1
                            total_size = 0
                        
                        insert_item(file_path, checksum, last_modified, batch_number, 0, status)
                        total_size += file_size
                        pbar.update(1)
                    except (IOError, OSError) as e:
                        logging.error(f"Error processing file {file_path}: {str(e)}")
    
    if incremental:
        cursor.execute('''
        INSERT INTO items (path, checksum, last_modified, batch_number, batch_complete, is_directory, run_number, status)
        SELECT path, checksum, last_modified, ?, 0, is_directory, ?, 'deleted'
        FROM items
        WHERE run_number = ? AND path NOT IN (SELECT path FROM items WHERE run_number = ?)
        ''', (batch_number, run_number, last_run_number, run_number))
    
    conn.commit()
    
    logging.info(f"Total items processed: {items_processed}")
    logging.info(f"Total batches created: {batch_number}")

def copy_and_checksum(src: str, dst: str, buffer_size: int = 1024 * 1024) -> Tuple[str, str]:
    src_checksum = hashlib.md5()
    dst_checksum = hashlib.md5()

    shutil.copy2(src, dst)

    # Calculate checksums
    for filename in (src, dst):
        checksum = src_checksum if filename == src else dst_checksum
        with open(filename, 'rb') as f:
            while True:
                buf = f.read(buffer_size)
                if not buf:
                    break
                checksum.update(buf)

    return src_checksum.hexdigest(), dst_checksum.hexdigest()

def copy_single_file(src: str, dst: str, conn: sqlite3.Connection, max_retries: int = 3, buffer_size: int = 1024 * 1024) -> bool:
    for attempt in range(max_retries):
        try:
            src_checksum, dst_checksum = copy_and_checksum(src, dst, buffer_size)
            if src_checksum != dst_checksum:
                raise ValueError(f"Checksum mismatch: {src} -> {dst}")
            
            c = conn.cursor()
            c.execute('UPDATE items SET checksum = ? WHERE path = ?', (dst_checksum, src))
            conn.commit()
            
            logging.info(f"Successfully copied and verified: {src} -> {dst}")
            return True
        except Exception as e:
            logging.error(f"Error copying {src} to {dst}, attempt {attempt + 1}: {str(e)}")
            if os.path.exists(dst):
                try:
                    os.remove(dst)
                    logging.info(f"Removed incomplete or incorrect copy: {dst}")
                except Exception as remove_error:
                    logging.error(f"Error removing incomplete copy {dst}: {str(remove_error)}")
        
        if attempt < max_retries - 1:
            time.sleep(1)
    
    return False

def fast_copy_with_sendfile(src: str, dst: str) -> bool:
    try:
        with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
            os.sendfile(fdst.fileno(), fsrc.fileno(), 0, os.path.getsize(src))
        return True
    except (AttributeError, OSError):
        shutil.copy2(src, dst)
        return False

def temporarily_change_permissions(file_path, callback):
    original_mode = os.stat(file_path).st_mode
    try:
        os.chmod(file_path, original_mode | stat.S_IWRITE)
        return callback()
    finally:
        os.chmod(file_path, original_mode)

def copy_single_file_optimized(src: str, dst: str, conn: sqlite3.Connection, run_number: int, max_retries: int = 3) -> bool:
    file_size = os.path.getsize(src)
    chunk_size = get_adaptive_chunk_size(file_size)
    
    for attempt in range(max_retries):
        try:
            def copy_operation():
                nonlocal src_checksum
                copy_large_file(src, dst, chunk_size)
                src_checksum = calculate_checksum(src, chunk_size)
                dst_checksum = calculate_checksum(dst, chunk_size)
                if src_checksum != dst_checksum:
                    raise ValueError(f"Checksum mismatch: {src} -> {dst}")
                
                # Set file timestamps and permissions
                st = os.stat(src)
                os.utime(dst, (st.st_atime, st.st_mtime))
                shutil.copymode(src, dst)
                
                return True

            src_checksum = None
            success = temporarily_change_permissions(src, copy_operation)
            if success:
                c = conn.cursor()
                c.execute('UPDATE items SET checksum = ? WHERE path = ? AND run_number = ?', (src_checksum, src, run_number))
                conn.commit()
                return True
        except Exception as e:
            logging.error(f"Error copying {src} to {dst}, attempt {attempt + 1}: {str(e)}")
            if os.path.exists(dst):
                try:
                    os.remove(dst)
                    logging.info(f"Removed incomplete or incorrect copy: {dst}")
                except Exception as remove_error:
                    logging.error(f"Error removing incomplete copy {dst}: {str(remove_error)}")
        
        if attempt < max_retries - 1:
            time.sleep(1)
    
    return False

def update_directory_times(conn: sqlite3.Connection, source_dir: str, dest_dir: str, run_number: int) -> None:
    c = conn.cursor()
    c.execute('SELECT path, last_modified FROM items WHERE is_directory = 1 AND run_number = ? AND status != "deleted" ORDER BY path DESC', (run_number,))
    directories = c.fetchall()
    
    for source_path, last_modified in tqdm(directories, desc='Updating directory timestamps'):
        relative_path = os.path.relpath(source_path, source_dir)
        dest_path = os.path.join(dest_dir, relative_path)
        
        if os.path.exists(dest_path):
            os.utime(dest_path, (last_modified, last_modified))
        else:
            logging.warning(f'Directory not found in destination: {dest_path}')

def create_all_directories(conn: sqlite3.Connection, source_dir: str, dest_dir: str, run_number: int) -> None:
    c = conn.cursor()
    c.execute('SELECT path FROM items WHERE is_directory = 1 AND run_number = ? ORDER BY path', (run_number,))
    directories = c.fetchall()
    
    for (dir_path,) in tqdm(directories, desc='Creating directories'):
        relative_path = os.path.relpath(dir_path, source_dir)
        dest_path = os.path.join(dest_dir, relative_path)
        try:
            if os.path.isdir(dest_path):
                current_mode = os.stat(dest_path).st_mode
                if not (current_mode & stat.S_IWUSR):
                    new_mode = current_mode | stat.S_IWUSR
                    os.chmod(dest_path, new_mode)
                    logging.info(f"Added write permission to existing directory: {dest_path}")
            else:
                os.makedirs(dest_path, exist_ok=True)
                os.chmod(dest_path, os.stat(dest_path).st_mode | stat.S_IWUSR)
                logging.info(f"Created new directory with write permission: {dest_path}")
        except Exception as e:
            logging.error(f'Failed to create or modify directory {dest_path}: {str(e)}')

def restore_directory_permissions(conn: sqlite3.Connection, source_dir: str, dest_dir: str, run_number: int) -> None:
    c = conn.cursor()
    c.execute('SELECT path FROM items WHERE is_directory = 1 AND run_number = ? ORDER BY path DESC', (run_number,))
    directories = c.fetchall()
    
    for (dir_path,) in tqdm(directories, desc='Restoring directory permissions'):
        relative_path = os.path.relpath(dir_path, source_dir)
        dest_path = os.path.join(dest_dir, relative_path)
        try:
            if os.path.isdir(dest_path):
                source_mode = os.stat(dir_path).st_mode
                os.chmod(dest_path, source_mode)
                logging.info(f"Restored original permissions for directory: {dest_path}")
        except Exception as e:
            logging.error(f'Failed to restore permissions for directory {dest_path}: {str(e)}')

def create_directories_for_batch(conn: sqlite3.Connection, source_dir: str, dest_dir: str, batch_number: int, run_number: int) -> None:
    c = conn.cursor()
    c.execute('''
    SELECT DISTINCT path FROM items 
    WHERE is_directory = 1 AND batch_number = ? AND run_number = ? 
    ORDER BY path
    ''', (batch_number, run_number))
    directories = c.fetchall()
    
    for (dir_path,) in directories:
        relative_path = os.path.relpath(dir_path, source_dir)
        dest_path = os.path.join(dest_dir, relative_path)
        try:
            if not os.path.isdir(dest_path):
                os.makedirs(dest_path, exist_ok=True)
            
            # Copy permissions, but don't set timestamps yet
            shutil.copymode(dir_path, dest_path)
            
            #logging.info(f"Created directory: {dest_path}")
        except Exception as e:
            logging.error(f'Failed to create directory {dest_path}: {str(e)}')

def copy_files_for_batch(conn: sqlite3.Connection, source_dir: str, dest_dir: str, batch_number: int, run_number: int, exclude_patterns: List[str], pbar: tqdm) -> None:
    c = conn.cursor()
    c.execute('''
    SELECT path, checksum, last_modified, is_directory, status 
    FROM items 
    WHERE batch_number = ? AND run_number = ?
    ''', (batch_number, run_number))
    items = c.fetchall()
    
    for path, checksum, last_modified, is_directory, status in items:
        if should_exclude(path, exclude_patterns) or status in ['unchanged', 'deleted'] or is_directory:
            pbar.update(1)
            continue

        relative_path = os.path.relpath(path, source_dir)
        dest_path = os.path.join(dest_dir, relative_path)

        if status in ['added', 'updated']:
            try:
                file_size = os.path.getsize(os.path.join(source_dir, path))
                if copy_single_file_optimized(os.path.join(source_dir, path), dest_path, conn, run_number):
                    #logging.info(f"Copied file: {path} -> {dest_path}")
                    pbar.set_postfix(file=os.path.basename(path), size=format_size(file_size), refresh=False)
                else:
                    logging.error(f'Failed to copy {path} after multiple attempts')
            except Exception as e:
                logging.error(f'Error copying file {path}: {str(e)}')
        
        pbar.update(1)

def format_size(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def set_directory_timestamps(conn: sqlite3.Connection, source_dir: str, dest_dir: str, run_number: int, pbar: tqdm) -> None:
    c = conn.cursor()
    c.execute('''
    SELECT path, last_modified FROM items 
    WHERE is_directory = 1 AND run_number = ? 
    ORDER BY path DESC
    ''', (run_number,))
    directories = c.fetchall()
    
    for dir_path, last_modified in directories:
        relative_path = os.path.relpath(dir_path, source_dir)
        dest_path = os.path.join(dest_dir, relative_path)
        try:
            os.utime(dest_path, (last_modified, last_modified))
            #logging.info(f"Set timestamp for directory: {dest_path}")
        except Exception as e:
            logging.error(f'Failed to set timestamp for directory {dest_path}: {str(e)}')
        pbar.update(1)

def restore_directory_permissions_for_batch(conn: sqlite3.Connection, source_dir: str, dest_dir: str, batch_number: int, run_number: int) -> None:
    c = conn.cursor()
    c.execute('''
    SELECT DISTINCT path FROM items 
    WHERE is_directory = 1 AND batch_number = ? AND run_number = ? 
    ORDER BY path DESC
    ''', (batch_number, run_number))
    directories = c.fetchall()
    
    for (dir_path,) in directories:
        relative_path = os.path.relpath(dir_path, source_dir)
        dest_path = os.path.join(dest_dir, relative_path)
        try:
            if os.path.isdir(dest_path):
                source_mode = os.stat(dir_path).st_mode
                os.chmod(dest_path, source_mode)
                #logging.info(f"Restored original permissions for directory: {dest_path}")
        except Exception as e:
            logging.error(f'Failed to restore permissions for directory {dest_path}: {str(e)}')

def get_incomplete_batches(conn: sqlite3.Connection, start_batch: int, end_batch: Optional[int], run_number: int) -> Tuple[List[Tuple[int]], int]:
    c = conn.cursor()
    if end_batch is not None:
        c.execute('''
        SELECT DISTINCT batch_number 
        FROM items 
        WHERE batch_complete = 0 
        AND batch_number BETWEEN ? AND ? 
        AND run_number = ? 
        ORDER BY batch_number
        ''', (start_batch, end_batch, run_number))
    else:
        c.execute('''
        SELECT DISTINCT batch_number 
        FROM items 
        WHERE batch_complete = 0 
        AND batch_number >= ? 
        AND run_number = ? 
        ORDER BY batch_number
        ''', (start_batch, run_number))
    incomplete_batches = c.fetchall()
    
    # Get total number of batches
    c.execute('SELECT MAX(batch_number) FROM items WHERE run_number = ?', (run_number,))
    total_batches = c.fetchone()[0] or 0
    
    return incomplete_batches, total_batches

def copy_items_in_batches(conn: sqlite3.Connection, source_dir: str, dest_dir: str, start_batch: int, end_batch: Optional[int], run_number: int, exclude_patterns: List[str] = []) -> None:
    incomplete_batches, total_batches = get_incomplete_batches(conn, start_batch, end_batch, run_number)
    
    for index, (batch_number,) in enumerate(incomplete_batches, 1):
        total_items, file_count, total_size = get_batch_info(conn, batch_number, run_number, source_dir)
        logging.info(f'Starting copy of batch {batch_number} (Batch {index} of {len(incomplete_batches)}, Total batches: {total_batches})')
        logging.info(f'Batch info: {file_count} files, total size: {format_size(total_size)}')
        
        create_directories_for_batch(conn, source_dir, dest_dir, batch_number, run_number)
        
        with tqdm(total=total_items, desc=f'Batch {batch_number}/{total_batches} ({file_count} files, {format_size(total_size)})') as pbar:
            copy_files_for_batch(conn, source_dir, dest_dir, batch_number, run_number, exclude_patterns, pbar)
        
        # Mark the batch as complete
        c = conn.cursor()
        c.execute('UPDATE items SET batch_complete = 1 WHERE batch_number = ? AND run_number = ?', (batch_number, run_number))
        conn.commit()
        logging.info(f'Batch {batch_number} marked as complete')

    # Second pass: Set directory timestamps
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM items WHERE is_directory = 1 AND run_number = ?', (run_number,))
    total_directories = c.fetchone()[0]
    with tqdm(total=total_directories, desc='Setting directory timestamps') as pbar:
        set_directory_timestamps(conn, source_dir, dest_dir, run_number, pbar)

    handle_deletions(conn, source_dir, dest_dir, run_number)
    
    logging.info(f"All {total_batches} batches have been processed for run {run_number}")

def handle_deletions(conn: sqlite3.Connection, source_dir: str, dest_dir: str, run_number: int) -> None:
    c = conn.cursor()
    c.execute('''
    SELECT path, is_directory FROM items 
    WHERE run_number = ? AND status = 'deleted' 
    ORDER BY path DESC
    ''', (run_number,))
    deleted_items = c.fetchall()

    for path, is_directory in deleted_items:
        dest_path = os.path.join(dest_dir, os.path.relpath(path, source_dir))
        if os.path.exists(dest_path):
            try:
                if is_directory:
                    shutil.rmtree(dest_path)
                else:
                    os.remove(dest_path)
                logging.info(f'Deleted: {dest_path}')
            except Exception as e:
                logging.error(f'Failed to delete {dest_path}: {str(e)}')

def validate_local(conn: sqlite3.Connection, source_dir: str, run_number: int) -> None:
    c = conn.cursor()
    c.execute('''
    SELECT path, checksum, last_modified, batch_number, is_directory, status 
    FROM items 
    WHERE run_number = ?
    ''', (run_number,))
    items = c.fetchall()
    
    invalid_batches = set()
    unexpected_items = []
    
    for item_path, db_checksum, db_last_modified, batch_number, is_directory, status in tqdm(items, desc='Validating local items'):
        if status == 'deleted':
            if os.path.exists(item_path):
                logging.warning(f"Item marked as deleted but still exists: {item_path}")
                unexpected_items.append((item_path, 'directory' if is_directory else 'file'))
                invalid_batches.add(batch_number)
            continue
        
        if os.path.exists(item_path):
            current_last_modified = os.path.getmtime(item_path)
            if abs(current_last_modified - db_last_modified) > 1:
                logging.warning(f"Timestamp mismatch detected for {'directory' if is_directory else 'file'}: {item_path}")
                logging.warning(f"  Database last modified: {db_last_modified}")
                logging.warning(f"  Current last modified: {current_last_modified}")
                invalid_batches.add(batch_number)
            elif not is_directory:
                current_checksum = calculate_checksum(item_path, 1024*1024)
                if current_checksum != db_checksum:
                    logging.warning(f"Checksum mismatch detected for file: {item_path}")
                    logging.warning(f"  Database checksum: {db_checksum}")
                    logging.warning(f"  Current checksum: {current_checksum}")
                    invalid_batches.add(batch_number)
        else:
            logging.error(f"{'Directory' if is_directory else 'File'} not found: {item_path}")
            invalid_batches.add(batch_number)
    
    if invalid_batches:
        logging.warning("\nThe following batches need to be re-run:")
        for batch in sorted(invalid_batches):
            logging.warning(f"Batch {batch}")
    
    if unexpected_items:
        logging.warning("\nThe following items should have been deleted but still exist:")
        for item, item_type in unexpected_items:
            logging.warning(f"Unexpected {item_type}: {item}")
    
    if not invalid_batches and not unexpected_items:
        logging.info("\nAll items validated successfully.")
    else:
        logging.info(f"\nValidation complete. Found {len(invalid_batches)} invalid batches and {len(unexpected_items)} items that should have been deleted.")

def get_latest_incomplete_run(conn: sqlite3.Connection) -> Optional[int]:
    c = conn.cursor()
    c.execute('''
    SELECT run_number 
    FROM backup_runs 
    WHERE end_time IS NULL 
    ORDER BY start_time DESC 
    LIMIT 1
    ''')
    result = c.fetchone()
    return result[0] if result else None

def validate_remote(conn: sqlite3.Connection, source_dir: str, dest_dir: str, run_number: int, time_tolerance: int = 3) -> None:
    c = conn.cursor()
    c.execute('''
    SELECT path, last_modified, is_directory, status 
    FROM items 
    WHERE run_number = ?
    ''', (run_number,))
    items = c.fetchall()
    
    mismatches = []
    missing_items = []
    unexpected_items = []
    
    for item_path, db_last_modified, is_directory, status in tqdm(items, desc='Validating remote items'):
        relative_path = os.path.relpath(item_path, source_dir)
        dest_path = os.path.join(dest_dir, relative_path)
        
        if status == 'deleted':
            if os.path.exists(dest_path):
                unexpected_items.append((dest_path, 'directory' if is_directory else 'file'))
            continue
        
        if not os.path.exists(dest_path):
            missing_items.append((item_path, 'directory' if is_directory else 'file'))
            continue
        
        src_stat = os.stat(item_path)
        dst_stat = os.stat(dest_path)
        
        if not is_directory and src_stat.st_size != dst_stat.st_size:
            mismatches.append((item_path, 'size mismatch', src_stat.st_size, dst_stat.st_size))
            continue
        
        if abs(src_stat.st_mtime - dst_stat.st_mtime) > time_tolerance:
            mismatches.append((item_path, 'time mismatch', src_stat.st_mtime, dst_stat.st_mtime))
    
    if mismatches:
        logging.warning(f"\nThe following items have mismatched attributes (time tolerance: {time_tolerance} seconds):")
        for path, mismatch_type, src_value, dst_value in mismatches:
            logging.warning(f"{'Directory' if os.path.isdir(path) else 'File'}: {path}")
            if mismatch_type == 'size mismatch':
                logging.warning(f"  Size mismatch - Source: {src_value} bytes, Destination: {dst_value} bytes")
            else:
                logging.warning(f"  Time mismatch - Source: {src_value}, Destination: {dst_value}")
    
    if missing_items:
        logging.warning("\nThe following items are missing from the destination:")
        for item, item_type in missing_items:
            logging.warning(f"Missing {item_type}: {item}")
    
    if unexpected_items:
        logging.warning("\nThe following items should have been deleted but still exist in the destination:")
        for item, item_type in unexpected_items:
            logging.warning(f"Unexpected {item_type}: {item}")
    
    if not mismatches and not missing_items and not unexpected_items:
        logging.info("\nAll items validated successfully on the destination.")
    else:
        logging.info(f"\nValidation complete. Found {len(mismatches)} mismatches, {len(missing_items)} missing items, and {len(unexpected_items)} items that should have been deleted.")

def get_latest_run_number(conn: sqlite3.Connection) -> int:
    c = conn.cursor()
    c.execute("SELECT MAX(run_number) FROM backup_runs")
    result = c.fetchone()[0] 
    return result if result is not None else 0

def get_total_batches(conn: sqlite3.Connection, run_number: int) -> int:
    c = conn.cursor()
    c.execute('SELECT MAX(batch_number) FROM items WHERE run_number = ?', (run_number,))
    result = c.fetchone()[0]
    return result if result is not None else 0

def end_run(conn: sqlite3.Connection, run_number: int) -> None:
    c = conn.cursor()
    c.execute("UPDATE backup_runs SET end_time = ? WHERE run_number = ?",
              (datetime.now(), run_number))
    conn.commit()

def resume_batch(conn: sqlite3.Connection, source_dir: str, dest_dir: str) -> Optional[int]:
    run_number = get_latest_incomplete_run(conn)
    if run_number is None:
        logging.info("No incomplete backup runs found. Starting a new backup.")
        return None

    exclude_patterns = get_exclude_patterns(conn, run_number)
    logging.info(f"Resuming run {run_number} with exclude patterns: {exclude_patterns}")

    incomplete_batches, total_batches = get_incomplete_batches(conn, 1, None, run_number)
    
    if not incomplete_batches:
        logging.info(f"No incomplete batches found for run {run_number}. The backup is already complete.")
        return None
    
    first_incomplete_batch = incomplete_batches[0][0]
    logging.info(f"Resuming run {run_number} from batch {first_incomplete_batch} of {total_batches}")
    cleanup_incomplete_batch(conn, source_dir, dest_dir, first_incomplete_batch, run_number)
    copy_items_in_batches(conn, source_dir, dest_dir, first_incomplete_batch, None, run_number, exclude_patterns)
    end_run(conn, run_number)

    return run_number

def cleanup_incomplete_batch(conn: sqlite3.Connection, source_dir: str, dest_dir: str, batch_number: int, run_number: int) -> None:
    c = conn.cursor()
    c.execute('''
    SELECT path, is_directory, status 
    FROM items 
    WHERE batch_number = ? AND run_number = ? AND status IN ('added', 'updated')
    ''', (batch_number, run_number))
    items = c.fetchall()
    
    for path, is_directory, status in items:
        relative_path = os.path.relpath(path, source_dir)
        dest_path = os.path.join(dest_dir, relative_path)
        
        if not is_directory and os.path.exists(dest_path):
            try:
                os.remove(dest_path)
                logging.info(f"Removed incomplete file: {dest_path}")
            except Exception as e:
                logging.error(f"Failed to remove incomplete file {dest_path}: {str(e)}")

    c.execute('''
    UPDATE items 
    SET batch_complete = 0 
    WHERE batch_number = ? AND run_number = ?
    ''', (batch_number, run_number))
    conn.commit()

def get_db_filename(source_dir: str, dest_dir: str) -> str:
    source_dir = os.path.normpath(source_dir)
    dest_dir = os.path.normpath(dest_dir)
    combined_path = f"{source_dir}|{dest_dir}"
    hash_object = hashlib.md5(combined_path.encode())
    hash_hex = hash_object.hexdigest()
    return f"backup_{hash_hex}.db"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Advanced Backup Script with Multi-Folder Support')
    parser.add_argument('source_dir', help='Source directory for file transfer', type=str)
    parser.add_argument('dest_dir', help='Destination directory for file transfer', type=str)
    parser.add_argument('--start-batch', type=int, default=1, help='Batch number to start from')
    parser.add_argument('--end-batch', type=int, help='Batch number to end at (optional)')
    parser.add_argument('--exclude', nargs='*', default=[], help='Patterns to exclude (e.g., *.tmp)')
    parser.add_argument('--validate-local', action='store_true', help='Run local validation')
    parser.add_argument('--validate-remote', action='store_true', help='Run remote validation')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size in MB (default: 100)')
    parser.add_argument('--incremental', action='store_true', help='Run an incremental backup')
    parser.add_argument('--resume', action='store_true', help='Resume the most recent incomplete backup')
    args = parser.parse_args()

    exclude_patterns = args.exclude
    batch_size_bytes = args.batch_size * 1024 * 1024
    db_file = get_db_filename(args.source_dir, args.dest_dir)
    
    try:
        with get_db_connection(db_file) as conn:
            create_db(conn)
            
            if args.validate_local:
                latest_run = get_latest_run_number(conn)
                validate_local(conn, args.source_dir, latest_run)
            elif args.validate_remote:
                latest_run = get_latest_run_number(conn)
                validate_remote(conn, args.source_dir, args.dest_dir, latest_run, time_tolerance=3)
            elif args.resume:
                resumed_run = resume_batch(conn, args.source_dir, args.dest_dir)
                if resumed_run is None: 
                    run_type = 'incremental' if args.incremental else 'full'
                    run_number = start_new_run(conn, run_type, exclude_patterns)
                    scan_and_record_items(conn, args.source_dir, batch_size=batch_size_bytes, 
                                          exclude_patterns=exclude_patterns, run_number=run_number, 
                                          incremental=args.incremental)
                    copy_items_in_batches(conn, args.source_dir, args.dest_dir, 1, 
                                          None, run_number, exclude_patterns=exclude_patterns)
                    end_run(conn, run_number)
                logging.info(f"Backup completed. Run number: {resumed_run if resumed_run else run_number}")
            else:
                run_type = 'incremental' if args.incremental else 'full'
                run_number = start_new_run(conn, run_type, exclude_patterns)
                
                if not args.incremental or args.start_batch == 1:
                    scan_and_record_items(conn, args.source_dir, batch_size=batch_size_bytes, 
                                          exclude_patterns=exclude_patterns, run_number=run_number, 
                                          incremental=args.incremental)
                
                copy_items_in_batches(conn, args.source_dir, args.dest_dir, args.start_batch, 
                                      args.end_batch, run_number, exclude_patterns=exclude_patterns)
                
                end_run(conn, run_number)
                logging.info(f"Backup completed. Run number: {run_number}")
            
            logging.info(f"Database file is located at: {os.path.abspath(db_file)}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        sys.exit(1)