
# Data Migration Tool

## Overview

This is a Python-based data migration tool designed to efficiently synchronize files from a source to a destination directory. It provides a user-friendly graphical interface using `tkinter` and `ttkbootstrap`, allowing users to monitor the migration process, adjust bandwidth limits, and track synchronization history.

### Key Features
- **Bandwidth Rate Limiting**: Control the file transfer speed dynamically.
- **Incremental Synchronization**: Synchronizes only new or modified files using SQLite to track file metadata.
- **System Resource Monitoring**: Avoids high system load by monitoring CPU, memory, and disk usage.
- **File Checksum Validation**: Ensures data integrity through checksum verification (optional for large files).
- **Pause/Resume Support**: The transfer can be paused and resumed at any time.
- **Retry Mechanism**: Automatically retries failed file transfers.
- **Logging and History**: Provides real-time logging and a history of past synchronization events.

## Prerequisites

Before using the tool, ensure you have the following dependencies installed:

- Python 3.6+
- Required libraries:
  
  pip install -r requirements.txt

## Usage

1. **Clone this repository**:

    git clone https://github.com/Charisn/data-migration-tool.git
    cd data-migration-tool

2. **Configure Paths**:

   Edit the source and destination paths in the script:

   SRC_PATH = r'\\'                  # Source path (old server - Also supports local network ip paths.)
   DEST_PATH = r'\\'                 # Destination path (new server)

   *(Optional)* Customize settings such as transfer rate, sync interval, and retry settings.

3. **Run the Script**:
    python migration_tool.py

   Use the graphical interface to start, pause, and monitor the migration.

### Features

#### Bandwidth Control

You can adjust the transfer rate during the operation using the slider in the interface, which dynamically adjusts the allowed bandwidth for file transfers.

#### Pause/Resume

The migration can be paused and resumed at any time using the respective buttons in the GUI. In the case of an error, the tool will automatically retry the file transfer up to a specified number of times.

#### Manual Sync

The tool performs automatic synchronization at a set interval (default: every 48 hours). You can also initiate manual synchronization via the "Manual Sync" button in the GUI.

#### Logs and Sync History

All logs are written to a file in the `logs/` directory and are also displayed in the GUI. Past sync events are recorded in the history section of the UI, allowing you to track the status of previous operations.

### Customization

You can modify the following variables in the script:

- `DEFAULT_MAX_BYTES_PER_SECOND`: Sets the default transfer rate (in bytes per second).
- `SYNC_INTERVAL_HOURS`: Sets the interval for automatic synchronization.
- `MAX_WORKERS`: Defines the number of threads used for parallel file transfers.
- `MAX_RETRIES`: Sets the maximum number of retry attempts for file transfers.

## Future Enhancements

Potential improvements could include:

- Adding more granular control over which files to sync.
- Enhancing the checksum verification to make it optional for certain file sizes.
- Implementing notifications (e.g., email alerts) upon completion or failure.

## Contributing

Contributions are welcome! Please fork this repository, make your changes, and open a pull request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
