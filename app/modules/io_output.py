from io import BytesIO
import pandas as pd
from PIL import Image
from flask import flash, render_template


def io_output_txt_csv(df: pd.DataFrame, sep: str = ",", header: bool = False, index: bool = False) -> BytesIO:
    output = BytesIO()
    df = df.to_csv(header=header, index=index, sep=sep).encode()
    output.write(df)
    output.seek(0)
    return output


def io_output(df: pd.DataFrame, is_index=False) -> BytesIO:
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=is_index)
    writer.close()
    output.seek(0)
    return output


def io_img_output(img: Image.Image, dpi=(300, 300)) -> BytesIO:
    img_io = BytesIO()
    img.save(img_io, 'JPEG', quality=100, dpi=dpi)
    img_io.seek(0)
    return img_io


def io_audio_stream(audio_stream):
    audio_buffer = BytesIO()
    audio_stream.stream_to_buffer(audio_buffer)
    audio_buffer.seek(0)
    return audio_buffer
