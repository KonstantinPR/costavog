from io import BytesIO
import pandas as pd
from PIL import Image


def io_output(df: pd.DataFrame) -> BytesIO:
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer)
    writer.close()
    output.seek(0)
    return output


def io_output(df: pd.DataFrame) -> BytesIO:
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer)
    writer.close()
    output.seek(0)
    return output


def io_img_output(img: Image.Image) -> BytesIO:
    img_io = BytesIO()
    img.save(img_io, 'JPEG', quality=100)
    img_io.seek(0)
    return img_io


# def io_output_all(file_io):
#     file_io = BytesIO()
#     file_io.seek(0)
#     return file_io
