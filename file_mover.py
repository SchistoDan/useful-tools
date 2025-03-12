import os
import sys
import shutil

def move_files_with_string_in_name(input_dir, output_dir, string_in_filename):
    try:
        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Get a list of all files in the input directory
        files_in_directory = os.listdir(input_dir)

        # Filter files that contain the specified string in their filename
        matching_files = [file for file in files_in_directory if string_in_filename in file]

        if not matching_files:
            print(f"No files containing '{string_in_filename}' in their names were found in {input_dir}.")
            return

        # Move each matching file to the output directory
        for file in matching_files:
            source_path = os.path.join(input_dir, file)
            destination_path = os.path.join(output_dir, file)
            shutil.move(source_path, destination_path)
            print(f"Moved: {file} -> {destination_path}")

        print(f"All matching files have been moved to {output_dir}.")

    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def print_usage():
    print("Usage:")
    print("  python script.py <input_directory> <output_directory> <string_in_filename>")
    print("\nArguments:")
    print("  <input_directory>      The directory to search for files.")
    print("  <output_directory>     The directory to move the matching files to.")
    print("  <string_in_filename>   The string to search for in the filenames.")
    print("\nExample:")
    print("  python script.py /path/to/input /path/to/output _r_1.3_s_50_")

if __name__ == "__main__":
    # Check if the correct number of arguments is provided
    if len(sys.argv) != 4:
        print("Error: Incorrect number of arguments.")
        print_usage()
    else:
        input_dir = sys.argv[1]
        output_dir = sys.argv[2]
        string_in_filename = sys.argv[3]
        move_files_with_string_in_name(input_dir, output_dir, string_in_filename)
