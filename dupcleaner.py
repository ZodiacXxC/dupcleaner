import hashlib
import os
from datetime import datetime
import concurrent.futures
from threading import Lock

def get_file_creation_date(file_path):
    try:
        creation_time = os.path.getctime(file_path)
        return datetime.fromtimestamp(creation_time)
    except FileNotFoundError:
        print(f"[Error] File not found: {file_path}")
        return None

def calculate_sha256(file_path):
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as file:
            for chunk in iter(lambda: file.read(65536), b""):  # 64 KB buffer
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    except FileNotFoundError:
        print(f"[Error] File not found: {file_path}")
        return None

def find_duplicates_in_directory(dirpath, filenames, hashed_files, lock):
    duplicates = []
    for file_name in filenames:
        full_path = os.path.join(dirpath, file_name)
        file_hash = calculate_sha256(full_path)
        if file_hash:
            with lock:
                if file_hash in hashed_files:
                    original_file = hashed_files[file_hash]
                    creation_date_1 = get_file_creation_date(full_path)
                    creation_date_2 = get_file_creation_date(original_file)
                    if creation_date_1 and creation_date_2:
                        older_file = full_path if creation_date_1 > creation_date_2 else original_file
                        duplicates.append(older_file)
                        print(f"[Duplicate] {full_path} is a duplicate of {original_file}")
                else:
                    hashed_files[file_hash] = full_path
    return duplicates

def find_duplicates(root_path):
    if not os.path.isdir(root_path):
        print(f"[Error] Invalid directory: {root_path}")
        return

    hashed_files = {}
    duplicates = []
    lock = Lock()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for dirpath, _, filenames in os.walk(root_path):
            futures.append(executor.submit(find_duplicates_in_directory, dirpath, filenames, hashed_files, lock))
        
        for future in concurrent.futures.as_completed(futures):
            duplicates.extend(future.result())

    if duplicates:
        print(f"\nFound {len(duplicates)} duplicate files.")
        remove = input("Do you want to remove all duplicate files? [Y/n]: ").strip().lower()
        if remove in ["y", "yes"]:
            for file_path in duplicates:
                try:
                    os.remove(file_path)
                    print(f"[Removed] {file_path}")
                except Exception as e:
                    print(f"[Error] Could not remove {file_path}: {e}")
            print("Successfully removed all duplicates!")
        else:
            print("No files were removed.")
    else:
        print("No duplicate files found.")

if __name__ == "__main__":
    while True:
        files_path = input("Enter the path to check for duplicates[Enter (Exit) to close]: ").strip()
        if files_path == "exit" or files_path == "Exit":
            break
        find_duplicates(files_path)
