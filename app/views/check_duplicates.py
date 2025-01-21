import os
from collections import defaultdict
import chardet

def detect_file_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
        return result['encoding']

def find_duplicates_in_txt_files(directory, length_threshold):
    # Filter based on length
    line_locations = defaultdict(list)

    for subdir, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.txt'):
                file_path = os.path.join(subdir, file)
                print(f'Reading file: {file_path}')
                try:
                    encoding = detect_file_encoding(file_path)
                    with open(file_path, 'r', encoding=encoding) as f:
                        for line in f:
                            if len(line.rstrip('\n')) > length_threshold:  # Check line length for > 80
                                stripped_line = line.strip()
                                line_locations[stripped_line].append(file_path)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

    duplicates = {line: paths for line, paths in line_locations.items() if len(paths) > 1}
    return duplicates

def save_duplicates_to_file(duplicates, output_file):
    with open(output_file, 'w', encoding='utf-8') as f:
        if duplicates:
            for line, paths in duplicates.items():
                f.write(f'"{line}" appears in the following files:\n')
                for path in paths:
                    f.write(f'- {path}\n')
                f.write('\n')
        else:
            f.write("No duplicates found.\n")

if __name__ == '__main__':
    input_path = r"C:\YandexDisk\ПОСТАВЩИКИ\АКТУАЛЬНЫЕ\КОВРИГИНА\ПРИХОДЫ"
    output_file_path = r"C:\YandexDisk\duplicates_report.txt"
    length_threshold = 80  # Define threshold for filtering lines

    duplicates = find_duplicates_in_txt_files(input_path, length_threshold)
    save_duplicates_to_file(duplicates, output_file_path)

    print(f"Duplicate report has been saved to: {output_file_path}")