from PIL import Image, ImageOps
import numpy as np
from io import BytesIO
from app.modules import io_output
import zipfile

K_HEIGHT_LEFT_START = 0.10
K_HEIGHT_RIGHT_START = 0.90
K_BOTTOM_LEFT_START = 0.05
K_BOTTOM_RIGHT_START = 0.95
STEP_ITERATION = 10
SENSIBILITY_COLOR = 210
COUNT_STEP_J = 10
DELIMITER = 4


def crop_images(images):
    images_zipped = None
    images_set = []
    for img in images:
        file_name = img.filename
        file = _crop_img(img)
        print('file after _crop_img: ' + str(file))
        img = io_output.io_img_output(file)
        images_set.append((file_name, img))
        print(images_set)
        images_zipped = _put_in_zip(images_set)

    return images_zipped


def _crop_img(img):
    # rotate problem fixing
    original_image = Image.open(img)
    fixed_image = ImageOps.exif_transpose(original_image)
    img = fixed_image
    pix = np.array(img)
    img__crop_height_top = _crop_height(pix, STEP_ITERATION)
    img__crop_height_bottom, left_border, right_border = _crop_bottom(pix, img__crop_height_top)
    area = (left_border, img__crop_height_top, right_border, img__crop_height_bottom)  # left, top, right, bottom
    cropped_img = ImageOps.crop(img, area)
    im = cropped_img

    # Унифицировать разрешение по высоте до

    old_width_im, old_height_im = im.size
    new_height_im = 2950
    k_widht_height_im = 0.75
    new_width_im = int(new_height_im * k_widht_height_im)
    if old_height_im < old_width_im:
        new_height_im = int(old_height_im * (new_width_im) / old_width_im)

    new_width_im = int(new_height_im / old_height_im * old_width_im)
    pic_size = (new_width_im, new_height_im)
    im = im.resize(pic_size)
    print("new_height_im= " + str(new_height_im))
    print("new_width_im= " + str(new_width_im))

    white_pic_size = (2250, 3000)
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

    return (new_im)


# crop image
def _crop_height(img, STEP_ITERATION):
    height, width, color = img.shape
    step = STEP_ITERATION
    left_start = width * K_HEIGHT_LEFT_START
    right_end = width * K_HEIGHT_RIGHT_START
    new_height = 0
    count_i = step * COUNT_STEP_J
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


def _crop_bottom(img, img__crop_height_top):
    height, width, color = img.shape
    step = STEP_ITERATION
    left_start = width * K_BOTTOM_LEFT_START
    right_end = width * K_BOTTOM_RIGHT_START
    new_bottom = 0
    count_i = step * 10
    left_right_border = []
    for i in img[img__crop_height_top + step * 10::step]:
        count_j = step * COUNT_STEP_J
        count_row = 0
        for j in i:
            if left_start < count_j < right_end:
                if sum(j) > SENSIBILITY_COLOR * 3:

                    count_row += 1
                    if count_row >= (
                            width * K_BOTTOM_RIGHT_START - width * K_BOTTOM_LEFT_START) - 5 and count_i > height / DELIMITER:
                        print(new_bottom)
                        new_bottom = height - img__crop_height_top - count_i - step
                        break
                else:
                    left_right_border.append(count_j)
            count_j += 1
            if new_bottom != 0:
                break
        count_i += step
    left_border = min(left_right_border) - step * 12
    right_border = width - (max(left_right_border) + step * 12)
    print("new bottom " + str(new_bottom))
    print("left_border " + str(left_border))
    print("right_border" + str(right_border))
    return new_bottom, left_border, right_border


def _put_in_zip(images):
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, data in images:
            zip_file.writestr(file_name, data.getvalue())
    zip_buffer.seek(0)
    print(zip_buffer)
    return zip_buffer
