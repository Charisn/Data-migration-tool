import os
import time
import threading
import hashlib
import logging
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import messagebox, simpledialog
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3

# Import ttkbootstrap for modern UI
import ttkbootstrap as ttk
from ttkbootstrap.constants import *

# ------------- User Configurable Variables -------------
# Paths can be edited here
SRC_PATH = r'\\'                  # Source path (old live server)
DEST_PATH = r'\\'                 # Destination path (new live server)

DEFAULT_MAX_BYTES_PER_SECOND = 50 * 1024 * 1024  # Defaults to 50 MB/s transfer rate limit
SYNC_INTERVAL_HOURS = 48                         # Synchronization interval in hours
FILE_DB_PATH = 'file_db.sqlite'                  # Path to file metadata database (using SQLite)
LOG_DIR_PATH = 'logs'                            # Directory to store logs
MAX_RETRIES = 3                                  # Max retries for copying a file
RETRY_DELAY = 5                                  # Delay between retries in seconds
MAX_WORKERS = os.cpu_count() * 2                 # Number of threads for parallel operations
# -------------------------------------------------------

# Ensure the log directory exists
os.makedirs(LOG_DIR_PATH, exist_ok=True)

# Setup logging
logging.basicConfig(
    filename=os.path.join(LOG_DIR_PATH, 'migration.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Global variables
current_max_bytes_per_second = DEFAULT_MAX_BYTES_PER_SECOND
pause_event = threading.Event()  # Event to control pausing
stop_event = threading.Event()   # Event to control stopping

class RateLimiter:
    """
    Centralized rate limiter to control the total bandwidth used across threads,
    dynamically adjusts to changes in max_bytes_per_second.
    """
    def __init__(self, max_bytes_per_second):
        self.max_bytes_per_second = max_bytes_per_second
        self.bytes_transferred = 0
        self.lock = threading.Lock()
        self.start_time = time.time()

    def acquire(self, num_bytes):
        global current_max_bytes_per_second  # Access dynamic limit
        while True:
            with self.lock:
                # Refresh max_bytes_per_second dynamically
                self.max_bytes_per_second = current_max_bytes_per_second
                elapsed = time.time() - self.start_time
                if elapsed >= 1:
                    # Reset every second
                    self.bytes_transferred = 0
                    self.start_time = time.time()
                if self.bytes_transferred + num_bytes <= self.max_bytes_per_second:
                    self.bytes_transferred += num_bytes
                    return  # Allow transfer
            # Wait briefly if over limit, allowing the cap to adjust dynamically
            time.sleep(0.01)

def compute_checksum(file_path, algorithm='sha256'):
    """
    Compute the checksum of a file using the specified algorithm.
    """
    hash_func = hashlib.new(algorithm)
    try:
        with open(file_path, 'rb') as f:
            while True:
                if stop_event.is_set():
                    return None
                chunk = f.read(8192)
                if not chunk:
                    break
                hash_func.update(chunk)
        return hash_func.hexdigest()
    except Exception as e:
        logging.error(f'Error computing checksum for {file_path}: {e}')
        return None

def copy_file_with_rate_limit(src_path, dest_path, rate_limiter):
    """
    Copy a file from src_path to dest_path using a shared rate limiter.
    """
    buffer_size = 4 * 1024 * 1024  # 4 MB buffer size
    try:
        with open(src_path, 'rb') as src_file, open(dest_path, 'wb') as dest_file:
            while True:
                if stop_event.is_set():
                    return
                # Check for pause
                while pause_event.is_set():
                    if stop_event.is_set():
                        return
                    time.sleep(1)
                chunk = src_file.read(buffer_size)
                if not chunk:
                    break
                # Acquire permission to transfer
                rate_limiter.acquire(len(chunk))
                dest_file.write(chunk)
    except Exception as e:
        logging.error(f'Error copying file {src_path} to {dest_path}: {e}')
        raise

def get_file_metadata(file_path):
    """
    Get the size and modification time of a file.
    """
    try:
        stats = os.stat(file_path)
        return {
            'size': stats.st_size,
            'mtime': stats.st_mtime
        }
    except Exception as e:
        logging.error(f'Error accessing file metadata for {file_path}: {e}')
        raise

def init_db(db_path):
    """
    Initialize the SQLite database for storing file metadata.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS file_metadata (
            path TEXT PRIMARY KEY,
            size INTEGER,
            mtime REAL
        )
    ''')
    conn.commit()
    return conn

def get_prev_metadata(conn, relative_path):
    """
    Retrieve previous metadata for a file from the database.
    """
    c = conn.cursor()
    c.execute('SELECT size, mtime FROM file_metadata WHERE path = ?', (relative_path,))
    result = c.fetchone()
    if result:
        return {'size': result[0], 'mtime': result[1]}
    else:
        return None

def update_file_metadata(conn, metadata_updates):
    """
    Update the metadata of files in the database in batch.
    """
    c = conn.cursor()
    c.executemany('REPLACE INTO file_metadata (path, size, mtime) VALUES (?, ?, ?)', metadata_updates)
    conn.commit()

def is_system_under_high_load(threshold=80.0):
    """
    Check if the system is under high CPU, disk, or memory load.
    """
    cpu_usage = psutil.cpu_percent(interval=1)
    disk_usage = psutil.disk_usage('/').percent
    memory_usage = psutil.virtual_memory().percent
    return cpu_usage > threshold or disk_usage > threshold or memory_usage > threshold

class MigrationApp(ttk.Window):
    def __init__(self):
        super().__init__(title="Data Migration Tool", themename="darkly")
        self.geometry("800x700")
        self.resizable(False, False)

        # Variables
        self.progress_value = tk.DoubleVar()
        self.next_sync_time = datetime.now() + timedelta(hours=SYNC_INTERVAL_HOURS)
        self.sync_history = []
        self.sync_in_progress = False
        self.is_shutting_down = False  # Track if shutdown is in progress
        self.sync_thread = None

        # Create UI elements
        self.create_widgets()

        # Set up custom log handler for the text widget
        text_log_handler = TextLogHandler(self.log_text)
        text_log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(text_log_handler)

        # Optionally, add file handler to write to a file as well
        file_handler = logging.FileHandler(os.path.join(LOG_DIR_PATH, 'migration.log'))
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.getLogger().addHandler(file_handler)

        logging.getLogger().setLevel(logging.DEBUG)

        # Start countdown update
        self.update_countdown()

    def create_widgets(self):
        # Bandwidth control
        bandwidth_frame = ttk.Frame(self)
        bandwidth_frame.pack(fill="x", padx=10, pady=5)

        ttk.Label(bandwidth_frame, text="Transfer Rate (MB/s):").pack(side="left", padx=5)
        self.bandwidth_var = tk.DoubleVar(value=DEFAULT_MAX_BYTES_PER_SECOND / (1024 * 1024))
        bandwidth_slider = ttk.Scale(bandwidth_frame, from_=1, to=100, variable=self.bandwidth_var, command=self.update_bandwidth)
        bandwidth_slider.pack(side="left", fill="x", expand=True, padx=5)

        self.bandwidth_label = ttk.Label(bandwidth_frame, text=f"{self.bandwidth_var.get():.1f} MB/s")
        self.bandwidth_label.pack(side="left", padx=5)

        # Progress display
        progress_frame = ttk.Frame(self)
        progress_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_value, maximum=100, bootstyle="info-striped")
        self.progress_bar.pack(fill="x", padx=10, pady=10)

        self.progress_label = ttk.Label(progress_frame, text="Progress: 0%")
        self.progress_label.pack()

        # Sync history
        history_frame = ttk.Frame(self)
        history_frame.pack(fill="both", expand=True, padx=10, pady=5)

        columns = ("timestamp", "status")
        self.history_tree = ttk.Treeview(history_frame, columns=columns, show="headings", height=8)
        self.history_tree.heading("timestamp", text="Timestamp")
        self.history_tree.heading("status", text="Status")
        self.history_tree.column("timestamp", width=200)
        self.history_tree.column("status", width=100)
        self.history_tree.pack(fill="both", expand=True)

        # Next sync countdown
        self.countdown_label = ttk.Label(self, text="Next synchronization in: --:--:--")
        self.countdown_label.pack(pady=5)

        # Loading indicator for evaluating files
        self.loading_label = ttk.Label(self, text="Evaluating files for synchronization...", bootstyle="info")
        self.loading_label.pack(pady=5)
        self.loading_label.pack_forget()  # Initially hidden

        # Indeterminate progress bar as a loading indicator
        self.loading_indicator = ttk.Progressbar(self, mode='indeterminate', bootstyle="info")
        self.loading_indicator.pack(pady=5)
        self.loading_indicator.pack_forget()  # Initially hidden

        # Control buttons
        self.control_frame = ttk.Frame(self)
        self.control_frame.pack(pady=5)

        self.start_button = ttk.Button(self.control_frame, text="Start Migration", command=self.start_migration, bootstyle="success")
        self.start_button.pack(side="left", padx=5)

        self.pause_button = ttk.Button(self.control_frame, text="Pause", command=self.pause_migration, state="disabled", bootstyle="warning")
        self.pause_button.pack(side="left", padx=5)

        self.resume_button = ttk.Button(self.control_frame, text="Resume", command=self.resume_migration, state="disabled", bootstyle="info")
        self.resume_button.pack(side="left", padx=5)

        # Manual Sync button
        self.manual_sync_button = ttk.Button(self.control_frame, text="Manual Sync", command=self.manual_sync, bootstyle="primary")
        self.manual_sync_button.pack(side="left", padx=5)

        # Log display area
        log_frame = ttk.Frame(self)
        log_frame.pack(fill="both", expand=True, padx=10, pady=5)

        self.log_text = tk.Text(log_frame, height=10, wrap="word", state="disabled")
        self.log_text.pack(fill="both", expand=True)

    def manual_sync(self):
        if not self.sync_in_progress:
            self.next_sync_time = datetime.now() + timedelta(hours=SYNC_INTERVAL_HOURS)
            self.start_synchronization()

    def update_bandwidth(self, event=None):
        global current_max_bytes_per_second
        value = self.bandwidth_var.get()
        current_max_bytes_per_second = value * 1024 * 1024  # Convert MB/s to bytes/s
        self.bandwidth_label.config(text=f"{value:.1f} MB/s")

    def update_progress(self, percentage):
        self.progress_value.set(percentage)
        self.progress_label.config(text=f"Progress: {percentage:.1f}%")

    def update_countdown(self):
        if not self.sync_in_progress:
            # Calculate remaining time until the next sync
            remaining = self.next_sync_time - datetime.now()
            if remaining.total_seconds() > 0:
                hours, remainder = divmod(remaining.total_seconds(), 3600)
                minutes, seconds = divmod(remainder, 60)
                self.countdown_label.config(text=f"Next synchronization in: {int(hours):02}:{int(minutes):02}:{int(seconds):02}")
            else:
                # Start synchronization and reset the sync time
                self.start_synchronization()
                self.next_sync_time = datetime.now() + timedelta(hours=SYNC_INTERVAL_HOURS)
        else:
            # Indicate synchronization in progress instead of countdown
            self.countdown_label.config(text="Synchronization in progress...")

        # Schedule the next countdown update in 1 second
        self.after(1000, self.update_countdown)

    def start_migration(self):
        if not self.sync_in_progress:
            confirm = messagebox.askyesno("Confirm", f"Source Path: {SRC_PATH}\nDestination Path: {DEST_PATH}\n\nDo you want to proceed?")
            if confirm:
                self.start_button.config(state="disabled")
                self.pause_button.config(state="normal")
                self.sync_in_progress = True
                self.sync_thread = threading.Thread(target=self.perform_migration)
                self.sync_thread.start()

    def start_synchronization(self):
        if not self.sync_in_progress:
            self.sync_in_progress = True
            self.manual_sync_button.config(state="disabled")  # Disable manual sync button
            self.loading_label.pack()  # Show loading label
            self.loading_indicator.pack()  # Show progress bar
            self.loading_indicator.start(10)  # Start indeterminate loading animation
            self.sync_thread = threading.Thread(target=self.perform_migration)
            self.sync_thread.start()

    def pause_migration(self):
        pause_event.set()
        self.pause_button.config(state="disabled")
        self.resume_button.config(state="normal")

    def resume_migration(self):
        pause_event.clear()
        self.pause_button.config(state="normal")
        self.resume_button.config(state="disabled")

    def perform_migration(self):
        try:
            logging.info('Starting synchronization')
            self.update_progress(0)
            conn = init_db(FILE_DB_PATH)
            rate_limiter = RateLimiter(current_max_bytes_per_second)

            start_time = datetime.now()
            log_filename = os.path.join(LOG_DIR_PATH, f"sync_{start_time.strftime('%Y%m%d_%H%M%S')}.log")
            sync_logger = logging.getLogger(f"sync_{start_time.strftime('%Y%m%d_%H%M%S')}")
            fh = logging.FileHandler(log_filename)
            fh.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            sync_logger.addHandler(fh)

            # Check if the database is empty or below 100KB
            if os.path.exists(FILE_DB_PATH) and os.path.getsize(FILE_DB_PATH) < 100 * 1024:
                first_migration = True
                sync_logger.info("Initial migration without metadata comparison.")
            else:
                first_migration = False
                # Load all metadata for quick lookups
                file_db = self.load_all_metadata(conn)

            files_to_copy = []
            metadata_updates = []

            # Begin evaluating files using parallel directory scanning
            self.loading_label.config(text="Scanning directories...")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                for dirpath, dirnames, filenames in os.walk(SRC_PATH):
                    if stop_event.is_set():
                        break
                    futures.append(executor.submit(self.scan_directory, dirpath, filenames, first_migration, file_db if not first_migration else None))

                # Collect results as they complete
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        files_to_copy.extend(result)

            # Evaluation complete, hide loading indicators
            self.loading_label.pack_forget()
            self.loading_indicator.stop()
            self.loading_indicator.pack_forget()

            total_files = len(files_to_copy)
            if total_files == 0:
                sync_logger.info("No new or updated files to copy.")
                sync_status = "No Changes"
            else:
                copied_files = 0
                # Copy files using ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_file = {}
                    for file_info in files_to_copy:
                        if stop_event.is_set():
                            break
                        src_path, dest_path, relative_path, src_metadata = file_info
                        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                        future = executor.submit(self.copy_file_with_retry, src_path, dest_path, rate_limiter, src_metadata, sync_logger, relative_path, first_migration)
                        future_to_file[future] = relative_path
                    for future in as_completed(future_to_file):
                        if stop_event.is_set():
                            break
                        while pause_event.is_set():
                            if stop_event.is_set():
                                break
                            time.sleep(1)
                        try:
                            result = future.result()
                            # Update progress
                            copied_files += 1
                            percentage = (copied_files / total_files) * 100 if total_files > 0 else 100
                            self.update_progress(percentage)
                        except Exception as e:
                            sync_logger.error(f'Failed to copy file: {e}')
                sync_status = "Success" if not stop_event.is_set() else "Stopped"

            end_time = datetime.now()
            duration = end_time - start_time
            if not self.is_shutting_down:  # Update history only if still active
                self.sync_history.append({
                    "timestamp": start_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "status": sync_status,
                    "log": log_filename
                })
            self.update_history()
            logging.info(f'Synchronization completed in {duration}')
            if sync_status == "Success":
                logging.info(f"Synchronization completed in {duration}")
            elif sync_status == "No Changes":
                logging.info("No new or updated files to synchronize.")
        except Exception as e:
            logging.error(f'Error during synchronization: {e}')
        finally:
            self.sync_in_progress = False
            conn.close()
            # Hide loading indicators and re-enable the manual sync button
            self.loading_label.pack_forget()
            self.loading_indicator.stop()
            self.loading_indicator.pack_forget()
            self.manual_sync_button.config(state="normal")  # Enable manual sync button again
            # Only reset GUI elements if shutdown is not in progress
            if not self.is_shutting_down:
                self.start_button.config(state="normal")
                self.pause_button.config(state="disabled")
                self.resume_button.config(state="disabled")
                self.update_progress(0)
                pause_event.clear()
                stop_event.clear()

    def scan_directory(self, dirpath, filenames, first_migration, file_db):
        files_to_copy = []

        # Handle empty directories
        if not filenames and not os.listdir(dirpath):  # Check if the directory is empty
            relative_path = os.path.relpath(dirpath, SRC_PATH)
            dest_path = os.path.join(DEST_PATH, relative_path)
            os.makedirs(dest_path, exist_ok=True)  # Ensure empty directory is created
            return []

        for filename in filenames:
            if stop_event.is_set():
                return []
            src_path = os.path.join(dirpath, filename)
            relative_path = os.path.relpath(src_path, SRC_PATH)
            dest_path = os.path.join(DEST_PATH, relative_path)

            src_metadata = get_file_metadata(src_path)

            if first_migration:
                # Skip metadata comparison
                files_to_copy.append((src_path, dest_path, relative_path, src_metadata))
            else:
                prev_metadata = file_db.get(relative_path)
                if prev_metadata != src_metadata:
                    files_to_copy.append((src_path, dest_path, relative_path, src_metadata))

        return files_to_copy

    def load_all_metadata(self, conn):
        """
        Load all file metadata into a dictionary for quick lookups.
        """
        cursor = conn.cursor()
        cursor.execute("SELECT path, size, mtime FROM file_metadata")
        metadata = {row[0]: {'size': row[1], 'mtime': row[2]} for row in cursor.fetchall()}
        return metadata

    def copy_file_with_retry(self, src_path, dest_path, rate_limiter, src_metadata, sync_logger, relative_path, first_migration):
        """
        Copy a file with retries and checksum verification.
        """
        # Each thread creates its own SQLite connection
        thread_conn = sqlite3.connect(FILE_DB_PATH)
        stop_logged = False  # Flag to track if stop event has been logged in this thread
        metadata_updates = []

        try:
            for attempt in range(MAX_RETRIES):
                if stop_event.is_set():
                    if not stop_logged:
                        stop_logged = True
                    break
                try:
                    copy_file_with_rate_limit(src_path, dest_path, rate_limiter)

                    if not first_migration:
                        # Verify checksums only if not the first migration
                        src_checksum = compute_checksum(src_path)
                        dest_checksum = compute_checksum(dest_path)
                        if src_checksum != dest_checksum or src_checksum is None or dest_checksum is None:
                            sync_logger.error(f'Checksum mismatch for {src_path}')
                            os.remove(dest_path)  # Remove corrupted file
                            raise ValueError('Checksum mismatch')
                    sync_logger.info(f'Successfully copied {src_path} to {dest_path}')
                    # Update file metadata in the database
                    update_file_metadata(thread_conn, [(relative_path, src_metadata['size'], src_metadata['mtime'])])
                    break  # Exit retry loop on success
                except Exception as e:
                    sync_logger.error(f'Error copying {src_path} to {dest_path}: {e}')
                    if attempt < MAX_RETRIES - 1:
                        sync_logger.info(f'Retrying in {RETRY_DELAY} seconds... (Attempt {attempt + 1}/{MAX_RETRIES})')
                        time.sleep(RETRY_DELAY)
                    else:
                        sync_logger.error(f'Failed to copy {src_path} after {MAX_RETRIES} attempts. Skipping.')
                        raise
        finally:
            thread_conn.close()

    def update_history(self):
        # Clear the treeview
        for row in self.history_tree.get_children():
            self.history_tree.delete(row)
        # Insert new history
        for item in self.sync_history:
            self.history_tree.insert("", "end", values=(item['timestamp'], item['status']))

    def on_closing(self):
        password = "ch1234"
        pwd = simpledialog.askstring("Password Required", "Enter password to close the application:", show='*')
        if pwd == password:
            if self.sync_in_progress:
                if messagebox.askokcancel("Quit", "A synchronization is in progress. Do you want to stop it and exit?"):
                    # Begin shutdown sequence
                    self.is_shutting_down = True
                    stop_event.set()  # Signal threads to stop
                    if pause_event.is_set():
                        pause_event.clear()  # Resume if paused to allow threads to exit
                    self.check_for_exit()  # Use after to check for background threads
            else:
                self.destroy()  # Close immediately if no sync is in progress
        elif pwd is None:
            return  # User canceled password prompt
        else:
            messagebox.showerror("Error", "Incorrect password. Application will not close.")

    def check_for_exit(self):
        # Check if the sync thread has completed
        if self.sync_thread is not None and self.sync_thread.is_alive():
            self.after(100, self.check_for_exit)  # Poll every 100 ms until sync_thread completes
        else:
            self.destroy()  # Destroy GUI once the sync thread has safely exited

class TextLogHandler(logging.Handler):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        log_entry = self.format(record)
        def append_log():
            self.text_widget.configure(state="normal")
            self.text_widget.insert("end", log_entry + "\n")
            self.text_widget.configure(state="disabled")
            self.text_widget.see("end")  # Scroll to the end of the log
        # Ensure thread-safe GUI updates
        self.text_widget.after(0, append_log)

def main():
    """
    Main function to start the application.
    """
    app = MigrationApp()
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()

if __name__ == '__main__':
    main()