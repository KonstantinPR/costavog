import os
from pathlib import Path


def delete_all_files_keep_folders(folder_path):
    """
    Delete all files in the specified folder and its subfolders,
    but keep all folders and subfolders intact
    """

    folder_path = Path(folder_path)

    if not folder_path.exists():
        print(f"❌ Folder not found: {folder_path}")
        return

    print(f"🧹 Cleaning: {folder_path}")
    print("=" * 60)

    # Count files before deletion
    total_files = 0
    total_folders = 0

    for root, dirs, files in os.walk(folder_path):
        total_folders += len(dirs)
        total_files += len(files)

    print(f"📊 Before cleaning:")
    print(f"  📁 Folders: {total_folders}")
    print(f"  📄 Files: {total_files}")
    print()

    # Delete all files (walk bottom-up to handle nested files first)
    deleted_count = 0
    failed_count = 0

    for root, dirs, files in os.walk(folder_path, topdown=False):
        for file_name in files:
            file_path = Path(root) / file_name
            try:
                file_path.unlink()
                deleted_count += 1
                rel_path = file_path.relative_to(folder_path)
                print(f"🗑️  Deleted: {rel_path}")
            except Exception as e:
                failed_count += 1
                print(f"⚠️  Could not delete: {file_name} - {e}")

    # Count after cleaning
    remaining_files = 0
    remaining_folders = 0

    for root, dirs, files in os.walk(folder_path):
        remaining_folders += len(dirs)
        remaining_files += len(files)

    print()
    print("=" * 60)
    print(f"✅ Cleaning complete!")
    print(f"  📁 Folders preserved: {remaining_folders}")
    print(f"  🗑️  Files deleted: {deleted_count}")
    if failed_count > 0:
        print(f"  ⚠️  Failed to delete: {failed_count}")
    print(f"  📄 Files remaining: {remaining_files}")
    print("=" * 60)

    # Show the folder structure after cleaning
    if remaining_folders > 0:
        print("\n📁 Folder structure preserved:")
        print("=" * 60)
        for root, dirs, files in os.walk(folder_path):
            level = root.replace(str(folder_path), '').count(os.sep)
            indent = '    ' * level
            folder_name = os.path.basename(root) or 'test'
            if level == 0:
                print(f"{indent}📁 {folder_name}/")
            else:
                print(f"{indent}├── {folder_name}/")
            for d in dirs:
                print(f"{indent}│   └── {d}/")


def delete_files_keep_folders_simple(folder_path):
    """
    Simple version - just delete files, keep folders
    """

    if not os.path.exists(folder_path):
        print(f"❌ Folder not found: {folder_path}")
        return

    print(f"🧹 Deleting files in: {folder_path}")
    print("=" * 60)

    deleted = 0
    failed = 0

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                os.remove(file_path)
                deleted += 1
                rel_path = os.path.relpath(file_path, folder_path)
                print(f"🗑️  Deleted: {rel_path}")
            except Exception as e:
                failed += 1
                print(f"⚠️  Could not delete: {file} - {e}")

    print()
    print("=" * 60)
    print(f"✅ Done!")
    print(f"  🗑️  Files deleted: {deleted}")
    if failed > 0:
        print(f"  ⚠️  Failed: {failed}")

    # Show remaining structure
    print("\n📁 Remaining folder structure:")
    print("=" * 60)
    for root, dirs, files in os.walk(folder_path):
        level = root.replace(folder_path, '').count(os.sep)
        indent = '    ' * level
        folder_name = os.path.basename(root)
        if folder_name:
            print(f"{indent}├── {folder_name}/")
        for d in dirs:
            print(f"{indent}│   └── {d}/")


def quick_clean(path):
    """
    Ultra simple - just delete all files, keep folders
    No progress messages, just the result
    """

    if not os.path.exists(path):
        print(f"❌ Path not found: {path}")
        return

    deleted = 0

    for root, dirs, files in os.walk(path):
        for file in files:
            try:
                os.remove(os.path.join(root, file))
                deleted += 1
            except:
                pass

    print(f"✅ Deleted {deleted} files from: {path}")
    print(f"📁 All folders preserved")


if __name__ == "__main__":
    folder_path = r"C:\YandexDisk\test"

    # Option 1: Full version with detailed output
    delete_all_files_keep_folders(folder_path)

    # Option 2: Simple version
    # delete_files_keep_folders_simple(folder_path)

    # Option 3: Ultra simple (no output per file)
    # quick_clean(folder_path)