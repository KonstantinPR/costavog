from PIL import Image, ImageOps, JpegImagePlugin
import numpy as np
from io import BytesIO
from app.modules import io_output
import zipfile
import PIL
from typing import Union
import os

# # shoes boot
# K_HEIGHT_LEFT_START = 0.10
# K_HEIGHT_RIGHT_START = 0.90
# K_BOTTOM_LEFT_START = 0.10
# K_BOTTOM_RIGHT_START = 0.97
# STEP_ITERATION = 25
# STEP_ITERATION_BOTTOM = 20
# SENSIBILITY_COLOR = 210
# COUNT_STEP_J = 10
# DELIMITER = 4
# NEW_HEIGHT_IM = 3000
# K_WIDTH_HEIGHT_IM = 0.75
# WHITE_BLOCK_WIDTH = int(NEW_HEIGHT_IM * K_WIDTH_HEIGHT_IM)
# WHITE_BLOCK_HEIGHT = NEW_HEIGHT_IM
# K_BOOT_LONG_LEGS = 4

# shoes leg
K_HEIGHT_LEFT_START = 0.15
K_HEIGHT_RIGHT_START = 0.85
K_BOTTOM_LEFT_START = 0.10
K_BOTTOM_RIGHT_START = 0.90
STEP_ITERATION = 25
STEP_ITERATION_BOTTOM = 20
SENSIBILITY_COLOR = 210
COUNT_STEP_J = 10
DELIMITER = 4
NEW_HEIGHT_IM = 3000
K_WIDTH_HEIGHT_IM = 0.75
WHITE_BLOCK_WIDTH = int(NEW_HEIGHT_IM * K_WIDTH_HEIGHT_IM)
WHITE_BLOCK_HEIGHT = NEW_HEIGHT_IM
K_BOOT_LONG_LEGS = 2

# full clothes

# K_HEIGHT_LEFT_START = 0.30
# K_HEIGHT_RIGHT_START = 0.70
# K_BOTTOM_LEFT_START = 0.20
# K_BOTTOM_RIGHT_START = 0.80
# STEP_ITERATION = 25
# STEP_ITERATION_BOTTOM = 20
# SENSIBILITY_COLOR = 210
# COUNT_STEP_J = 10
# DELIMITER = 4
# NEW_HEIGHT_IM = 3000
# K_WIDTH_HEIGHT_IM = 0.75
# WHITE_BLOCK_WIDTH = int(NEW_HEIGHT_IM * K_WIDTH_HEIGHT_IM)
# WHITE_BLOCK_HEIGHT = NEW_HEIGHT_IM
# K_BOOT_LONG_LEGS = 1


def crop_images(images: Union[list[PIL.JpegImagePlugin.JpegImageFile], list]) -> BytesIO:
    images_zipped = None
    images_set = []
    print(images)
    for img in images:
        file_name = os.path.basename(img.filename)
        print(file_name)
        file = _crop_img(img)
        print('file after _crop_img: ' + str(file))
        img = io_output.io_img_output(file)
        images_set.append((file_name, img))
        print(images_set)
        images_zipped = _put_in_zip(images_set)

    return images_zipped


def _crop_img(img: PIL.JpegImagePlugin.JpegImageFile) -> Image:
    if isinstance(img, PIL.JpegImagePlugin.JpegImageFile):
        original_image = img
    else:
        original_image = Image.open(img)
    # rotate problem fixing
    fixed_image = ImageOps.exif_transpose(original_image)
    img = fixed_image
    pix = np.array(img)
    img_crop_height_top = _crop_height(pix)
    img_crop_height_bottom, left_border, right_border = _crop_bottom(pix, img_crop_height_top)
    area = (left_border, img_crop_height_top, right_border, img_crop_height_bottom)  # left, top, right, bottom
    cropped_img = ImageOps.crop(img, area)
    im = cropped_img

    # Унифицировать разрешение по высоте до

    old_width_im, old_height_im = im.size
    new_height_im = NEW_HEIGHT_IM
    k_width_height_im = K_WIDTH_HEIGHT_IM
    new_width_im = int(new_height_im * k_width_height_im)
    if old_height_im < old_width_im:
        new_height_im = int(old_height_im * (new_width_im) / old_width_im)

    new_width_im = int(new_height_im / old_height_im * old_width_im)
    pic_size = (new_width_im, new_height_im)
    im = im.resize(pic_size)
    print("new_height_im= " + str(new_height_im))
    print("new_width_im= " + str(new_width_im))

    white_pic_size = (WHITE_BLOCK_WIDTH, WHITE_BLOCK_HEIGHT)
    print("white_pic_size " + str(white_pic_size))
    new_im = Image.new("RGB", white_pic_size, (255, 255, 255, 255))

    if old_height_im < old_width_im:
        k = int((white_pic_size[1] - pic_size[1]) * 0.10)
        new_im.paste(im, ((white_pic_size[0] - pic_size[0] + k) // 2,
                          white_pic_size[1] - pic_size[1]))
    else:
        k = 0
        new_im.paste(im, ((white_pic_size[0] - pic_size[0]) // 2,
                          (white_pic_size[1] - pic_size[1] + k) // 2))
    print(type(new_im))

    return new_im


# crop image
def _crop_height(img):
    height, width, color = img.shape
    step = STEP_ITERATION
    left_start = width * K_HEIGHT_LEFT_START
    right_end = width * K_HEIGHT_RIGHT_START
    new_height = 0
    count_i = step * COUNT_STEP_J * K_BOOT_LONG_LEGS
    print(f"count_i {count_i}")
    for i in img[step * 10::step]:
        count_j = 0
        for j in i:
            if left_start < count_j < right_end:
                if sum(j) < SENSIBILITY_COLOR * 3:
                    new_height = count_i - step * 4
                    break
            count_j += 1
            if new_height != 0:
                break
        count_i += step
    print("new_height " + str(new_height))
    return new_height


def _crop_bottom(img, img_crop_height_top):
    height, width, color = img.shape
    step = STEP_ITERATION_BOTTOM
    left_start = width * K_BOTTOM_LEFT_START
    right_end = width * K_BOTTOM_RIGHT_START
    new_bottom = 0
    count_i = step * 10
    left_right_border = []
    for i in img[img_crop_height_top + step * 10::step]:
        count_j = step * COUNT_STEP_J
        count_row = 0
        for j in i:
            if left_start < count_j < right_end:
                if sum(j) > SENSIBILITY_COLOR * 3:
                    count_row += 1
                    if count_row >= (
                            width * K_BOTTOM_RIGHT_START - width * K_BOTTOM_LEFT_START) - 5 and count_i > height / DELIMITER:
                        print(new_bottom)
                        new_bottom = height - img_crop_height_top - count_i - step
                        break
                else:
                    left_right_border.append(count_j)
            count_j += 1
            if new_bottom != 0:
                break
        count_i += step
    left_border = min(left_right_border) - step * 12
    right_border = width - (max(left_right_border) + step * 2)
    print("new bottom " + str(new_bottom))
    print("left_border " + str(left_border))
    print("right_border" + str(right_border))
    return new_bottom, left_border, right_border


def _put_in_zip(images: list[tuple[str, BytesIO]]) -> BytesIO:
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, data in images:
            zip_file.writestr(file_name, data.getvalue())
    zip_buffer.seek(0)
    print(zip_buffer)
    return zip_buffer
