from PIL import Image, ImageOps
import numpy as np

k_height_left_start = 0.10
k_height_right_start = 0.90
k_bottom_left_start = 0.05
k_bottom_right_start = 0.95
STEP_ITERATION = 10
sensibility_color = 210
count_step_j = 10
delimiter = 4


def crop_img(img):
    # rotate problem fixing
    original_image = Image.open(img)
    fixed_image = ImageOps.exif_transpose(original_image)
    img = fixed_image
    pix = np.array(img)
    img_crop_height_top = crop_height(pix, STEP_ITERATION)
    img_crop_height_bottom, left_border, right_border = crop_bottom(pix, img_crop_height_top)
    area = (left_border, img_crop_height_top, right_border, img_crop_height_bottom)  # left, top, right, bottom
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
def crop_height(img, STEP_ITERATION):
    height, width, color = img.shape
    step = STEP_ITERATION
    left_start = width * k_height_left_start
    right_end = width * k_height_right_start
    new_height = 0
    count_i = step * count_step_j
    for i in img[step * 10::step]:
        count_j = 0
        for j in i:
            if left_start < count_j < right_end:
                if sum(j) < sensibility_color * 3:
                    new_height = count_i - step * 4
                    break
            count_j += 1
            if new_height != 0:
                break
        count_i += step
    print("new_height " + str(new_height))
    return new_height


def crop_bottom(img, img_crop_height_top):
    height, width, color = img.shape
    step = STEP_ITERATION
    left_start = width * k_bottom_left_start
    right_end = width * k_bottom_right_start
    new_bottom = 0
    count_i = step * 10
    left_right_border = []
    for i in img[img_crop_height_top + step * 10::step]:
        count_j = step * count_step_j
        count_row = 0
        for j in i:
            if left_start < count_j < right_end:
                if sum(j) > sensibility_color * 3:

                    count_row += 1
                    if count_row >= (
                            width * k_bottom_right_start - width * k_bottom_left_start) - 5 and count_i > height / delimiter:
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
    right_border = width - (max(left_right_border) + step * 12)
    print("new bottom " + str(new_bottom))
    print("left_border " + str(left_border))
    print("right_border" + str(right_border))
    return new_bottom, left_border, right_border
