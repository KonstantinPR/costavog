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
    "m4": "150 x 300 см.",
    "m5": "150 x 500 см.",
    "m6": "150 x 500 см.",
    "m7": "150 x 500 см.",
    "m9": "150 x 500 см.",
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


#
# def img_foldering(df):
#     # print(f"file_txt {df}")
#
#     images_folder = app.config['YANDEX_FOLDER_IMAGE']
#     folder_folders = "folder_img"
#
#     select = request.form.get('multiply_number')
#     typeWB_OZON = select
#     typeWB_OZON = 0 if typeWB_OZON == 'WB' else 1
#
#     shutil.rmtree(folder_folders, ignore_errors=True)
#
#     if not os.path.exists(folder_folders):
#         os.makedirs(folder_folders)
#
#     img_name_list_files = {}
#
#     for entry in os.scandir(images_folder):
#         for subentry in os.scandir(entry.path):
#             if subentry.is_dir():
#                 for file in os.scandir(subentry.path):
#                     if file.is_file():
#                         img_name_list_files[file.name] = subentry.path
#
#     # print(f"files {img_name_list_files}")
#
#     for i in df['Article']:
#         os.makedirs(f"{folder_folders}/{i}/photo")
#
#     val = df['Article'].values[0]
#     # print(f"Article_by_index {val}")
#
#     for name, path in img_name_list_files.items():
#         # name_clear = re.sub(r'(-9)?-\d.JPG', '', name)
#         name_clear = re.sub(r'-(\d)?\d.JPG', '', name)
#         for j in os.listdir(folder_folders):
#             j_clear = j
#             if j.startswith(tuple(PREF_LIST)) or j.endswith("new"):
#                 j_clear = j[:(len(j) - 3)]
#                 j_clear_end = j[(len(j) - 3):]
#                 # print(f"j_clear_end {j_clear_end}")
#             if name_clear == j_clear:
#                 if typeWB_OZON == 0:
#                     shutil.copyfile(f"{img_name_list_files[name]}/{name}", f"{folder_folders}/{j}/photo/{name}")
#                     if j.startswith(tuple(PREF_LIST)):
#                         img_watermark(f"{folder_folders}/{j}/photo/{name}", j)
#                 if typeWB_OZON == 1:
#                     shutil.copyfile(f"{img_name_list_files[name]}/{name}", f"{folder_folders}/{name}")
#
#     for j in os.listdir(folder_folders):
#         for d in range(len(df.index)):
#             if df['Article'][d] == j:
#                 # os.rename(f"{folder_folders}/{j}", f"{folder_folders}/{df['Article_WB'][d]}")
#                 # for updating wb on 20.09.2022
#                 os.rename(f"{folder_folders}/{j}", f"{folder_folders}/{df['Article'][d]}")
#
#     if typeWB_OZON == 1:
#         for j in os.listdir(folder_folders):
#             if j.endswith('-1.JPG'):
#                 os.rename(f"{folder_folders}/{j}", f"{folder_folders}/{j.replace('-1.JPG', '.JPG')}")
#             else:
#                 os.rename(f"{folder_folders}/{j}", f"{folder_folders}/{'_'.join(j.rsplit('-', 1))}")
#
#     shutil.make_archive(folder_folders, 'zip', f"{folder_folders}")
#     shutil.move(f"{folder_folders}.zip", folder_folders)
#
#     zip_file = os.path.abspath(f"{folder_folders}\{folder_folders}.zip")
#
#     return_data = io.BytesIO()
#     with open(zip_file, 'rb') as file:
#         return_data.write(file.read())
#     # (after writing, cursor will be at last byte, so move it to start)
#     return_data.seek(0)
#
#     shutil.rmtree(folder_folders, ignore_errors=True)
#
#     return return_data


def create_folder_structure(df):
    """
    Creates a folder structure for images based on the unique Article values in the given dataframe.
    """
    folder_path = app.config['TMP_IMG_FOLDER']
    os.makedirs(folder_path, exist_ok=True)

    for article in df["Article"]:
        os.makedirs(f"{folder_path}/{article}/photo", exist_ok=True)

    return folder_path


# def get_image_files(images_folder):
#     """
#     Scans a given folder and returns a dictionary of image filenames and their corresponding paths.
#     """
#     image_files = {}
#
#     for entry in os.scandir(images_folder):
#         for subentry in os.scandir(entry.path):
#             if subentry.is_dir():
#                 for file in os.scandir(subentry.path):
#                     if file.is_file():
#                         image_files[file.name] = subentry.path
#     return image_files


def get_image_files(images_folder):
    """
    Scans a given folder and returns a dictionary of image filenames and their corresponding paths.
    """
    image_files = {}
    image_files_duplicates = {}
    number_images_of_art = {}

    for entry in os.scandir(images_folder):
        for subentry in os.scandir(entry.path):
            if subentry.is_dir():
                for file in os.scandir(subentry.path):
                    if file.is_file():
                        art = re.sub(r'-(\d)?\d.JPG', '', file.name)
                        if art in number_images_of_art.keys():
                            number_images_of_art[art] = number_images_of_art[art] + 1
                        else:
                            number_images_of_art[art] = 1

                        if file.name not in image_files.keys():
                            image_files[file.name] = subentry.path
                            image_files_duplicates[file.name] = file.name
                        else:
                            new_name_img_file = art + f"-{number_images_of_art[art] + 1}.JPG"
                            image_files_duplicates[new_name_img_file] = file.name
                            image_files[new_name_img_file] = subentry.path

    print(image_files_duplicates)
    print(f"image_files.items() {image_files.keys()}")
    # exit()
    return image_files, image_files_duplicates


#
# def copy_images_to_folders(image_files, folder_path, typeWB_OZON):
#     """
#     Copies image files to folders in the specified folder path based on their corresponding Article value.
#     """
#     for name, path in image_files.items():
#         name_clear = re.sub(r'-(\d)?\d.JPG', '', name)
#
#         for folder_name in os.listdir(folder_path):
#             folder_name_clear = folder_name
#
#             if folder_name.startswith(tuple(PREF_LIST)) or folder_name.endswith("new"):
#                 folder_name_clear = folder_name[:(len(folder_name) - 3)]
#                 folder_name_clear_end = folder_name[(len(folder_name) - 3):]
#
#             if name_clear == folder_name_clear:
#                 if typeWB_OZON == 0:
#                     shutil.copyfile(f"{image_files[name]}/{name}", f"{folder_path}/{folder_name}/photo/{name}")
#                     if folder_name.startswith(tuple(PREF_LIST)):
#                         img_watermark(f"{folder_path}/{folder_name}/photo/{name}", folder_name)
#
#                 if typeWB_OZON == 1:
#                     shutil.copyfile(f"{image_files[name]}/{name}", f"{folder_path}/{name}")


def copy_images_to_folders(image_files, folder_path, typeWB_OZON):
    """
    Copies image files to folders in the specified folder path based on their corresponding Article value.
    """
    image_files, duplicate_images = image_files
    for name, pat in image_files.items():
        name_clear = re.sub(r'-(\d)?\d.JPG', '', name)

        for idx, folder_name in enumerate(os.listdir(folder_path)):
            # print(f"folder_name {folder_name}")
            folder_name_clear = folder_name

            if folder_name.startswith(tuple(PREF_LIST)) or folder_name.endswith("new"):
                folder_name_clear = folder_name[:(len(folder_name) - 3)]
                folder_name_clear_end = folder_name[(len(folder_name) - 3):]

            if name_clear == folder_name_clear:
                if typeWB_OZON == 0:
                    name = duplicate_images[name]
                    shutil.copyfile(f"{image_files[name]}/{name}", f"{folder_path}/{folder_name}/photo/{name}")
                    if folder_name.startswith(tuple(PREF_LIST)):
                        img_watermark(f"{folder_path}/{folder_name}/photo/{name}", folder_name)

                if typeWB_OZON == 1:
                    shutil.copyfile(f"{image_files[name]}/{name}", f"{folder_path}/{name}")


def rename_folders(df, folder_path, typeWB_OZON):
    """
    Renames folders based on the Article values in the given dataframe.
    """
    for folder_name in os.listdir(folder_path):
        for d in range(len(df.index)):
            if df["Article"][d] == folder_name:
                os.rename(f"{folder_path}/{folder_name}", f"{folder_path}/{df['Article'][d]}")

    if typeWB_OZON == 1:
        for folder_name in os.listdir(folder_path):
            if folder_name.endswith('-1.JPG'):
                os.rename(f"{folder_path}/{folder_name}", f"{folder_path}/{folder_name.replace('-1.JPG', '.JPG')}")
            else:
                os.rename(f"{folder_path}/{folder_name}", f"{folder_path}/{'_'.join(folder_name.rsplit('-', 1))}")


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


def img_foldering(df):
    images_folder = app.config["YANDEX_FOLDER_IMAGE"]

    typeWB_OZON = 0

    # Create folder structure
    folder_path = create_folder_structure(df)

    # Get image files
    image_files = get_image_files(images_folder)

    # Copy images to folders
    copy_images_to_folders(image_files, folder_path, typeWB_OZON)

    # Rename folders
    rename_folders(df, folder_path, typeWB_OZON)

    # Create zip file
    zip_file_data = create_zip_file(folder_path)
    shutil.rmtree(app.config['TMP_IMG_FOLDER'])

    return zip_file_data
