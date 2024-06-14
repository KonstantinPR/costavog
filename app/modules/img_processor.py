import logging
from app import app
from flask import request
import shutil
import re
import io
from PIL import Image, ImageDraw, ImageFont
import os
import img2pdf
import glob

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
}

SIZE_TRANSLATE_300 = {
    "m2": "300 x 200 см.",
    "m3": "300 x 300 см.",
    "m5": "300 x 500 см.",
}

PREF_LIST = ['FUR', 'LNF', 'WLP', 'GL0']


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
