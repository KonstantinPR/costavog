from PIL import ImageDraw, ImageFont
from app.modules import io_output
from barcode.writer import ImageWriter
from io import BytesIO
from barcode import Code128
from PIL import Image
from pylibdmtx.pylibdmtx import encode


def create_barcodes(df, type_barcode='code128'):
    lines = df[0].to_list()
    lines = [str(x) for x in lines]

    i = 0
    images_zipped = []
    images_set = []

    if len(lines[0]) > 100:
        type_barcode = 'Datamatrix'
        # flash("Code128 can't contain more then 100 simbol so type_barcode is chenged on Datamatrix")

    if type_barcode == 'code128':
        for line in lines:
            count_i = '{0:0>4}'.format(i)
            line_name = f'bar_{count_i}.png'

            rv = BytesIO()
            Code128(str(line), writer=ImageWriter()).write(rv)
            print(f'img {rv}')

            images_set.append((line_name, rv))
            i += 1

        return images_set

    for i, line in enumerate(lines):
        # print(line)
        count_i = '{0:0>4}'.format(i + 1)  # Change i to i + 1 to start counting from 1
        line_name = f"bar_{count_i}.png"
        scale_factor = 1  # Scale factor for making the image bigger
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
        # In the barcode generation code

        try:
            font = ImageFont.truetype("arial.ttf", 22 * scale_factor)
        except OSError:
            font = ImageFont.load_default()

        # Now use this font in the draw.text calls
        draw.text((14 * scale_factor, 6 * scale_factor), "ЧЕСТНЫЙ ЗНАК", font=font, fill=(0, 0, 0))
        draw.text((14 * scale_factor, new_height + 42 * scale_factor), line[0:18], font=font, fill=(0, 0, 0))
        draw.text((14 * scale_factor, new_height + 56 * scale_factor), line[18:31], font=font, fill=(0, 0, 0))
        draw.text((14 * scale_factor, new_height + 72 * scale_factor), line[32:38], font=font, fill=(0, 0, 0))

        img = io_output.io_img_output(padded_image, dpi=(300, 300))

        images_set.append((line_name, img))

    return images_set
