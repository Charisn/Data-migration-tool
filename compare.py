import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to get file size and path for all files in a directory
def get_all_files_with_size(directory):
    files_dict = {}
    for root, _, files in os.walk(directory):
        for file_name in files:
            full_file_path = os.path.join(root, file_name)
            try:
                file_size = os.path.getsize(full_file_path)
                relative_path = os.path.relpath(full_file_path, directory).lower()  # Normalize case for comparison
                files_dict[relative_path] = file_size
            except Exception as e:
                print(f"Error getting size of {full_file_path}: {e}")
    return files_dict

# Function to compare file sizes between two directories
def compare_file_sizes(dir1, dir2, max_workers=64):
    differences = []

    # Get all files with their sizes from both directories
    files_dict1 = get_all_files_with_size(dir1)
    files_dict2 = get_all_files_with_size(dir2)

    # Files only in dir1
    files_only_in_dir1 = set(files_dict1.keys()) - set(files_dict2.keys())
    for file_path in files_only_in_dir1:
        differences.append(f"Not exists: {os.path.join(dir1, file_path)} | {dir2}")

    # Files only in dir2
    files_only_in_dir2 = set(files_dict2.keys()) - set(files_dict1.keys())
    for file_path in files_only_in_dir2:
        differences.append(f"Not exists: {dir1} | {os.path.join(dir2, file_path)}")

    # Common files, compare sizes
    common_files = set(files_dict1.keys()) & set(files_dict2.keys())

    if common_files:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(compare_sizes, file_path, files_dict1[file_path], files_dict2[file_path], dir1, dir2): file_path
                for file_path in common_files
            }

            for future in as_completed(future_to_file):
                try:
                    result = future.result()
                    if result:
                        differences.append(result)
                except Exception as e:
                    differences.append(f"Error comparing files: {e}")

    return differences

# Function to compare sizes of a specific file in both directories
def compare_sizes(file_path, size1, size2, dir1, dir2):
    if size1 != size2:
        return f"Size difference: {os.path.join(dir1, file_path)} {size1} | {os.path.join(dir2, file_path)} {size2}"
    return None

# Function to write differences to file
def write_differences_to_file(differences, output_file):
    try:
        with open(output_file, 'w') as f:
            if differences:
                f.write("\n".join(differences))
            else:
                f.write("No differences found; the directories are identical.")
    except Exception as e:
        print(f"Error writing to file {output_file}: {e}")

# Main function to compare directories and output the results
def main():
    dir1 = r"\\192.168.123.250\s\Broker\24"
    dir2 = r"E:\Broker\24"
    output_file = r"E:\differences.txt"

    # Perform comparison
    differences = compare_file_sizes(dir1, dir2)

    # Output the results to a file
    write_differences_to_file(differences, output_file)
    print(f"Differences written to {output_file}")

if __name__ == "__main__":
    main()
