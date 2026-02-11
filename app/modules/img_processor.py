from app import app
import re
import io
from PIL import Image, ImageDraw, ImageFont
import os
import img2pdf
import glob
import shutil
from functools import wraps
from zipfile import ZipFile, ZIP_DEFLATED
import logging

SIZE_TRANSLATE_150 = {
    "m1": "150 x 100 см.",
    "m2": "150 x 200 см.",
    "m3": "150 x 300 см.",
    "m4": "150 x 400 см.",
    "m5": "150 x 500 см.",
    "m6": "150 x 600 см.",
    "m7": "150 x 700 см.",
    "m8": "150 x 800 см.",
    "m9": "150 x 900 см.",
    # "5QT": "5 шт.",
}

SIZE_TRANSLATE_300 = {
    "m2": "300 x 200 см.",
    "m3": "300 x 300 см.",
    "m5": "300 x 500 см.",
    # "5QT": "5 шт.",
}

PREF_LIST = ['FUR', 'LNF', 'WLP', 'GL0']

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_zip_of_zips(large_zip_obj, max_size_mb=500):
    """
    Create a single ZIP file containing multiple ZIP archives, preserving folder structure.
    Folders are never split across parts. Includes robust error handling.
    """
    MAX_SIZE_BYTES = max_size_mb * 1024 * 1024

    main_zip_buffer = io.BytesIO()
    original_zip = None
    # Get original zip content
    try:
        if hasattr(large_zip_obj, 'getvalue'):  # BytesIO object
            original_data = large_zip_obj.getvalue()
            original_zip = ZipFile(io.BytesIO(original_data))
            logging.info("Loaded original ZIP from BytesIO object.")
        else:  # File path
            with open(large_zip_obj, 'rb') as f:
                original_data = f.read()
            original_zip = ZipFile(io.BytesIO(original_data))
            logging.info(f"Loaded original ZIP from file: {large_zip_obj}")

        original_size = len(original_data)
        logging.info(f"Original ZIP size: {original_size / (1024 * 1024):.2f} MB")

        if original_size <= MAX_SIZE_BYTES:
            logging.info("Original ZIP is small enough, returning it directly.")
            main_zip_buffer.write(original_data)
            main_zip_buffer.seek(0)
            return main_zip_buffer

        # Build folder tree (exactly as before)
        folder_tree = {}
        file_list = sorted(original_zip.namelist())

        for filename in file_list:
            if filename.endswith('/'):  # Directory
                folder_tree[filename] = {'size': 0, 'files': [], 'subfolders': {}}
            else:  # File
                # Find parent folder
                parent_folder = filename.rsplit('/', 1)[0] + '/' if '/' in filename else ''
                if parent_folder not in folder_tree:
                    folder_tree[parent_folder] = {'size': 0, 'files': [], 'subfolders': {}}

                file_info = original_zip.getinfo(filename)
                file_size = file_info.file_size
                folder_tree[parent_folder]['files'].append((filename, file_size))
                folder_tree[parent_folder]['size'] += file_size + 100  # + overhead

        # Calculate total size for each folder (including subfolders)
        def calculate_folder_size(folder_path, tree):
            """Recursively calculate total size of folder including subfolders"""
            if folder_path not in tree:
                return 0

            folder = tree[folder_path]
            total_size = folder['size']  # Files in this folder

            # Add subfolder sizes
            for subfolder in folder['subfolders'].keys():
                subfolder_path = folder_path + subfolder if folder_path else subfolder
                total_size += calculate_folder_size(subfolder_path, tree)

            return total_size

        # Group files by their root folders
        root_folders = {}
        for folder_path in folder_tree:
            if not folder_path or folder_path.count('/') <= 1:  # Root or first-level folders
                total_folder_size = calculate_folder_size(folder_path, folder_tree)
                root_folders[folder_path] = {
                    'size': total_folder_size,
                    'path': folder_path,
                    'files': folder_tree[folder_path]['files'] if folder_path else []
                }

        logging.info(
            f"Found {len(root_folders)} root folder(s) with total size: {original_size / (1024 * 1024):.2f} MB")

        # Create "zip of zips" preserving folder structure
        with ZipFile(main_zip_buffer, 'w', ZIP_DEFLATED) as main_zip:  # Add deflation
            part_number = 1
            current_part_size = 0
            part_buffer = io.BytesIO()
            part_zip = None
            current_part_folders = []

            try:
                part_zip = ZipFile(part_buffer, 'w', ZIP_DEFLATED)  # Add deflation
                # Sort folders by size (largest first) to optimize packing
                sorted_folders = sorted(root_folders.items(), key=lambda x: x[1]['size'], reverse=True)

                for folder_path, folder_info in sorted_folders:
                    folder_size = folder_info['size']
                    logging.info(f"Processing folder '{folder_path}' (size: {folder_size / (1024 * 1024):.2f} MB)")

                    # Check if folder would exceed part limit
                    if (current_part_size + folder_size > MAX_SIZE_BYTES and
                            current_part_folders and
                            current_part_size > 0):
                        # Finalize current part
                        logging.info(
                            f"Part {part_number} full ({current_part_size / (1024 * 1024):.2f} MB), creating new part.")
                        part_zip.close()  # Ensure part_zip is closed before reading
                        part_buffer.seek(0)
                        part_size = len(part_buffer.getvalue())
                        part_filename = f'images_part_{part_number:03d}.zip'
                        main_zip.writestr(part_filename, part_buffer.getvalue())

                        logging.info(
                            f"Added part {part_number}: {part_size / (1024 * 1024):.2f} MB (contains {len(current_part_folders)} folders)")

                        # Reset for next part
                        current_part_folders = []
                        current_part_size = 0
                        part_number += 1
                        part_buffer = io.BytesIO()
                        part_zip = ZipFile(part_buffer, 'w', ZIP_DEFLATED)  # Add deflation

                    # Add entire folder to current part
                    logging.info(f"Adding folder '{folder_path}' to part {part_number}")

                    # Copy all files from this folder (and subfolders) to current part
                    def copy_folder_contents(zip_source, zip_target, folder_path):
                        """Recursively copy folder contents preserving structure"""
                        # Copy files in current folder
                        if folder_path in folder_tree:
                            for filename, _ in folder_tree[folder_path]['files']:
                                if filename != folder_path:  # Skip directory entry
                                    try:
                                        zip_target.writestr(filename, zip_source.read(filename))
                                        logging.debug(f"Copied file '{filename}'")  # Added debugging log
                                    except Exception as e:
                                        logging.error(f"Error copying file '{filename}': {e}")

                        # Find and copy subfolders
                        for potential_subfolder in folder_tree:
                            if (potential_subfolder.startswith(folder_path) and
                                    potential_subfolder.count('/') == folder_path.count('/') + 1):
                                # Copy files in subfolder
                                if potential_subfolder in folder_tree:
                                    for filename, _ in folder_tree[potential_subfolder]['files']:
                                        if filename != potential_subfolder:
                                            try:
                                                zip_target.writestr(filename, zip_source.read(filename))
                                                logging.debug(f"Copied file '{filename}'")  # Added debugging log
                                            except Exception as e:
                                                logging.error(f"Error copying file '{filename}': {e}")

                    copy_folder_contents(original_zip, part_zip, folder_path)
                    current_part_folders.append(folder_path)
                    current_part_size += folder_size

                # Add final part
                if current_part_folders:
                    logging.info(f"Creating the last zip part {part_number}")
                    part_zip.close()  # Ensure part_zip is closed before reading
                    part_buffer.seek(0)
                    part_size = len(part_buffer.getvalue())
                    part_filename = f'images_part_{part_number:03d}.zip'
                    main_zip.writestr(part_filename, part_buffer.getvalue())  # Add file to main zip

                    logging.info(
                        f"Added final part {part_number}: {part_size / (1024 * 1024):.2f} MB (contains {len(current_part_folders)} folders)")

                # Add detailed README
                readme_parts = []
                for i, (folder_path, folder_info) in enumerate(sorted_folders, 1):
                    part_num = ((i - 1) // max(1, len(sorted_folders) // 3 + 1)) + 1  # Rough part assignment
                    readme_parts.append(
                        f"Folder '{folder_path}' ({folder_info['size'] / (1024 * 1024):.2f} MB) -> part_{part_num:03d}.zip")

                info_content = f"""ZIP of ZIPs - Folder Structure Preserved
                                ========================================
                            
                                Total parts: {part_number}
                                Original ZIP size: {original_size / (1024 * 1024):.2f} MB
                                Each part < {max_size_mb} MB
                            
                                FOLDER DISTRIBUTION:
                                {chr(10).join(readme_parts)}
                            
                                INSTRUCTIONS:
                                1. Extract this main ZIP file
                                2. Each 'images_part_XXX.zip' contains COMPLETE folders with all their photos
                                3. Upload each part separately to your marketplace
                                4. NO folders are split - each folder stays intact in one part
                            
                                Folder structure inside each part ZIP is preserved exactly as in original.
                                """
                main_zip.writestr('README.txt', info_content)

            except Exception as e:
                logging.error("Error during ZIP part creation:", exc_info=True)  # Log full traceback
                raise  # Re-raise the error to stop further processing
            finally:
                if part_zip:
                    try:
                        part_zip.close()
                    except:
                        pass

    except Exception as e:
        logging.error("General error in create_zip_of_zips:", exc_info=True)  # Log full traceback
        raise  # Re-raise the error
    finally:
        if original_zip:
            try:
                original_zip.close()
            except:
                pass

    main_zip_buffer.seek(0)
    main_size = len(main_zip_buffer.getvalue())
    logging.info(f"Main 'zip of zips' size: {main_size / (1024 * 1024):.2f} MB")  # Changed print to logging

    # Flash message or similar notification
    total_folders = len(root_folders)
    logging.info(
        f"Created 'zip of zips' with {part_number} parts (total: {main_size / (1024 * 1024):.2f} MB). Preserved {total_folders} complete folders.")
    # flash(f"Created 'zip of zips' with {part_number} parts (total: {main_size / (1024*1024):.2f} MB). "
    #      f"Preserved {total_folders} complete folders. Extract and upload each part separately.", 'info')  #Commented this line

    return main_zip_buffer


# Add this decorator to clean up split files after response
def cleanup_split_files(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        response = f(*args, **kwargs)

        # Cleanup split files if they exist
        if 'split_files' in globals():
            split_files = globals().get('split_files', [])
            temp_dir = globals().get('split_temp_dir', None)

            # Keep the file being sent, remove others
            if split_files and hasattr(response, 'direct_passthrough') and response.direct_passthrough:
                current_file = None
                # Try to determine current file being sent
                for sf in split_files:
                    if os.path.exists(sf):
                        current_file = sf
                        break

                for sf in split_files:
                    if sf != current_file and os.path.exists(sf):
                        try:
                            os.unlink(sf)
                        except:
                            pass

            # Clear globals
            if 'split_files' in globals():
                del globals()['split_files']
            if 'split_temp_dir' in globals():
                del globals()['split_temp_dir']

        return response

    return decorated_function


def img_watermark(img_name, name):
    size = name[len(name) - 2:].lower()
    # print(f"size {size}")
    if name.startswith('GL0'):
        size_text = SIZE_TRANSLATE_300[size]
    else:
        size_text = SIZE_TRANSLATE_150[size]
    # print(f"size_text {size_text}")
    base = Image.open(img_name).convert('RGBA')
    width, height = base.size

    # make a blank image for the text, initialized to transparent text color
    # txt = Image.new('RGBA', base.size, (255, 255, 255, 0))
    txt = Image.new('RGBA', base.size, (50, 50, 50, 0))

    fontsize = 1  # starting font size

    # portion of image width you want text width to be
    img_fraction = 0.90
    text = size_text
    font = ImageFont.truetype("arial.ttf", fontsize)
    while font.getsize(text)[0] < img_fraction * base.size[0]:
        # iterate until the text size is just larger than the criteria
        fontsize += 1
        font = ImageFont.truetype("arial.ttf", fontsize)

    # optionally de-increment to be sure it is less than criteria
    fontsize -= 10

    # get a font
    fnt = ImageFont.truetype('arial.ttf', fontsize)
    # get a drawing context
    d = ImageDraw.Draw(txt)

    x = width * (1 - img_fraction)
    y = height - fontsize * 2

    # draw text, half opacity
    d.text((x, y), text, font=fnt, stroke_width=4, stroke_fill=(150, 150, 150, 200), fill=(255, 255, 255, 200))
    # d.text((x, y), text, font=fnt, fill=(50, 50, 50, 200))
    txt = txt.rotate(0)

    out = Image.alpha_composite(base, txt)
    out.convert('RGB').save(img_name, format="JPEG")


def images_into_pdf_1():
    """
    Не актуально, переписано на images_into_pdf2
    """
    path_pdf = "folder_img/output.pdf"
    with open(path_pdf, "wb") as f:
        f.write(img2pdf.convert(glob.glob("folder_img/*-1.jpg")))
    return path_pdf


def download_images_from_yandex_to_folder(df, art_col_name="Артикул товара"):
    # print(df[art_col_name])
    images_folder = app.config['YANDEX_FOLDER_IMAGE']
    folder_folders = "folder_img"
    shutil.rmtree(folder_folders, ignore_errors=True)

    if not os.path.exists(folder_folders):
        os.makedirs(folder_folders)

    img_name_list_files = {}

    for entry in os.scandir(images_folder):
        for subentry in os.scandir(entry.path):
            if subentry.is_dir():
                for file in os.scandir(subentry.path):
                    if file.is_file():
                        img_name_list_files[file.name] = subentry.path

    # print(df[art_col_name])
    for name, path in img_name_list_files.items():
        for jdx, j in enumerate(df[art_col_name]):
            # print(f'j is {j}')
            if j in name:
                shutil.copyfile(f"{img_name_list_files[name]}/{name}", f"{folder_folders}/{name}")
    print('download_images_from_yandex_to_folder is completed')
    return img_name_list_files


def create_folder_structure(df):
    """
    Creates a folder structure for images based on the unique Article values in the given dataframe.
    """
    folder_path = app.config['TMP_IMG_FOLDER']
    try:
        shutil.rmtree(app.config['TMP_IMG_FOLDER'])
        os.makedirs(folder_path, exist_ok=True)
        for article in df["Article"]:
            # os.makedirs(f"{folder_path}/{article}/photo", exist_ok=True)
            os.makedirs(f"{folder_path}/{article}", exist_ok=True)
    except:
        os.makedirs(folder_path, exist_ok=True)
        for article in df["Article"]:
            # os.makedirs(f"{folder_path}/{article}/photo", exist_ok=True)
            os.makedirs(f"{folder_path}/{article}", exist_ok=True)

    return folder_path


def _get_include_duplicates(file, subentry, set_img_dicts):
    image_files, renamed_duplicates, number_images_of_art = set_img_dicts

    art = re.sub(r'-(\d)?\d.JPG', '', file.name)
    if art in number_images_of_art.keys():
        number_images_of_art[art] += 1
    else:
        number_images_of_art[art] = 1

    if file.name not in image_files.keys():
        image_files[file.name] = subentry.path
        renamed_duplicates[file.name] = file.name
    else:
        new_name_img_file = art + f"-{number_images_of_art[art] + 1}.JPG"
        renamed_duplicates[new_name_img_file] = file.name
        image_files[new_name_img_file] = subentry.path

    return image_files, renamed_duplicates, number_images_of_art


def _get_exclude_duplicates(file, subentry, image_files, art_paths_dict):
    art = re.sub(r'-(\d)?\d.JPG', '', file.name)
    if file.name not in image_files.keys():
        if art not in art_paths_dict.keys(): art_paths_dict[art] = subentry.path
        if art_paths_dict[art] == subentry.path:
            image_files[file.name] = subentry.path
    return image_files


def order_by(entity, order_is='ASCENDING'):
    # print(order_is)
    if order_is == 'ASCENDING':
        return sorted(os.scandir(entity), key=lambda x: x.name, reverse=False)
    return sorted(os.scandir(entity), key=lambda x: x.name, reverse=True)


def get_image_files(images_folder: dict, is_replace: str, order_is: str) -> tuple:
    """
    Scans a given folder and returns a dictionary of image filenames and their corresponding paths.
    """
    image_files = {}
    renamed_duplicates = {}
    number_images_of_art = {}

    name_paths_dict = {}
    set_img_dicts = (image_files, renamed_duplicates, number_images_of_art)

    for entry in order_by(images_folder, order_is):
        for subentry in order_by(entry.path, order_is):
            if subentry.is_dir():
                for file in order_by(subentry.path):
                    if file.is_file():
                        if is_replace == "ALL":
                            set_img_dicts = _get_include_duplicates(file, subentry, set_img_dicts)
                        elif is_replace == "ONLY_NEW":
                            image_files = _get_exclude_duplicates(file, subentry, image_files, name_paths_dict)
                        else:
                            image_files = _get_exclude_duplicates(file, subentry, image_files, name_paths_dict)
    # print(f"renamed_duplicates  {renamed_duplicates}")
    # exit()
    return image_files, renamed_duplicates


def copy_images_to_folders(image_files, renamed_duplicates, folder_path, marketplace):
    """
    Copies image files to folders in the specified folder path based on their corresponding Article value.
    """
    folder_paths = {}
    for name, pat in image_files.items():
        name_clear = re.sub(r'-(\d)?\d.JPG', '', name)

        for idx, folder_name in enumerate(os.listdir(folder_path)):
            # print(f"folder_name {folder_name}")
            folder_name_clear = folder_name

            if folder_name.startswith(tuple(PREF_LIST)) or folder_name.endswith("new"):
                folder_name_clear = folder_name[:(len(folder_name) - 3)]
                folder_name_clear_end = folder_name[(len(folder_name) - 3):]

            if name_clear == folder_name_clear:
                if marketplace == "WB":
                    real_name = name
                    if renamed_duplicates: real_name = renamed_duplicates[name]
                    # shutil.copyfile(f"{image_files[name]}/{real_name}", f"{folder_path}/{folder_name}/photo/{name}")
                    shutil.copyfile(f"{image_files[name]}/{real_name}", f"{folder_path}/{folder_name}/{name}")
                    if folder_name.startswith(tuple(PREF_LIST)):
                        # img_watermark(f"{folder_path}/{folder_name}/photo/{name}", folder_name)
                        img_watermark(f"{folder_path}/{folder_name}/{name}", folder_name)

                if marketplace == "OZON":
                    shutil.copyfile(f"{image_files[name]}/{name}", f"{folder_path}/{name}")


def rename_folders(df, folder_path, marketplace):
    """
    Renames folders based on the Article values in the given dataframe.
    """
    article_wb_col_name = "Article"
    article_col_name = "Article"
    if not df["Article_WB"].empty:
        article_wb_col_name = "Article_WB"
        article_col_name = "Article"

    for folder_name in os.listdir(folder_path):
        for d in range(len(df.index)):
            if df[article_col_name][d] == folder_name:
                try:
                    os.rename(f"{folder_path}/{folder_name}", f"{folder_path}/{df[article_wb_col_name][d]}")
                except FileNotFoundError:
                    print(f"FileNotFoundError: Could not find or rename {folder_name} to {df[article_wb_col_name][d]}")
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")

    if marketplace == "OZON":
        for folder_name in os.listdir(folder_path):
            try:
                if folder_name.endswith('-1.JPG'):
                    os.rename(f"{folder_path}/{folder_name}", f"{folder_path}/{folder_name.replace('-1.JPG', '.JPG')}")
                else:
                    os.rename(f"{folder_path}/{folder_name}", f"{folder_path}/{'_'.join(folder_name.rsplit('-', 1))}")
            except FileNotFoundError:
                print(f"FileNotFoundError: Could not rename {folder_name}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")


def create_zip_file(folder_path):
    """
    Given a folder path, creates a zip archive of the folder and returns a BytesIO object containing the zip file data.
    """
    shutil.make_archive(folder_path, 'zip', f"{folder_path}")
    shutil.move(f"{folder_path}.zip", folder_path)

    zip_file = os.path.abspath(f"{folder_path}/{folder_path}.zip")

    return_data = io.BytesIO()
    with open(zip_file, 'rb') as file:
        return_data.write(file.read())

    return_data.seek(0)

    return return_data


def img_foldering(df, marketplace, is_replace, order_is):
    images_folder = app.config["YANDEX_FOLDER_IMAGE"]

    # Create folder structure
    folder_path = create_folder_structure(df)

    # Get image files
    image_files, renamed_duplicates = get_image_files(images_folder, is_replace, order_is)

    # Copy images to folders
    copy_images_to_folders(image_files, renamed_duplicates, folder_path, marketplace)

    # Rename folders
    rename_folders(df, folder_path, marketplace)

    # Create zip file
    zip_file_data = create_zip_file(folder_path)

    shutil.rmtree(app.config['TMP_IMG_FOLDER'])

    return zip_file_data
