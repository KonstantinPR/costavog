import requests
from PIL import ImageDraw, ImageFont, Image
import os
from app.modules import io_output
from barcode.writer import ImageWriter
from io import BytesIO
from barcode import Code128
from pylibdmtx.pylibdmtx import encode
from flask import current_app

def download_font(url, font_path):
    # Download the font file from the URL and save it locally
    response = requests.get(url)
    with open(font_path, 'wb') as f:
        f.write(response.content)

def create_barcodes(df, type_barcode='code128'):
    lines = df[0].to_list()
    lines = [str(x) for x in lines]

    i = 0
    images_set = []

    if len(lines[0]) > 100:
        type_barcode = 'Datamatrix'

    if type_barcode == 'code128':
        for line in lines:
            count_i = '{0:0>4}'.format(i)
            line_name = f'bar_{count_i}.png'

            rv = BytesIO()
            Code128(str(line), writer=ImageWriter()).write(rv)
            images_set.append((line_name, rv))
            i += 1

        return images_set

    scale_factor = 1
    font_url = "https://fontsforyou.com/fonts/a/Arial.ttf"
    font_path = os.path.join(current_app.static_folder, "Arial.ttf")

    if not os.path.exists(font_path):
        try:
            # Download the font if it doesn't exist locally
            download_font(font_url, font_path)
            print(f"Font downloaded from {font_url} to {font_path}")
        except Exception as e:
            print(f"Failed to download font: {e}")
            return []

    try:
        # Attempt to load the font from the downloaded file
        font22 = ImageFont.truetype(font_path, 22 * scale_factor)
        font16 = ImageFont.truetype(font_path, 16 * scale_factor)
    except OSError:
        print("Failed to load the font.")
        return []

    for i, line in enumerate(lines):
        count_i = '{0:0>4}'.format(i + 1)
        line_name = f"bar_{count_i}.png"
        encoded = encode(line.encode('utf8'))
        datamatrix_svg = Image.frombytes('RGB', size=(encoded.width, encoded.height), data=encoded.pixels)
        width, height = datamatrix_svg.width, datamatrix_svg.height

        new_width = width * scale_factor
        new_height = height * scale_factor

        # Create an image with a white background
        padded_image = Image.new(mode="RGB",
                                 size=(new_width + 10 * scale_factor, new_height + 100 * scale_factor),
                                 color=(255, 255, 255))

        # Paste the resized SVG image into the padded image
        datamatrix_svg = datamatrix_svg.resize((new_width, new_height), Image.LANCZOS)
        padded_image.paste(datamatrix_svg, (5 * scale_factor, 40 * scale_factor))

        # Draw text
        draw = ImageDraw.Draw(padded_image)

        # Draw the text with the loaded font
        draw.text((14 * scale_factor, 6 * scale_factor), "ЧЕСТНЫЙ ЗНАК", font=font22, fill=(0, 0, 0))
        draw.text((14 * scale_factor, new_height + 42 * scale_factor), line[0:18], font=font16, fill=(0, 0, 0))
        draw.text((14 * scale_factor, new_height + 56 * scale_factor), line[18:31], font=font16, fill=(0, 0, 0))
        draw.text((14 * scale_factor, new_height + 72 * scale_factor), line[32:38], font=font16, fill=(0, 0, 0))

        img = io_output.io_img_output(padded_image, dpi=(300, 300))

        images_set.append((line_name, img))

    return images_set
