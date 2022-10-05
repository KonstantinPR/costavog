from io import BytesIO
import zipfile


def put_in_zip(images: list[tuple[str, BytesIO]]) -> BytesIO:
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, data in images:
            zip_file.writestr(file_name, data.getvalue())
    zip_buffer.seek(0)
    print(zip_buffer)
    return zip_buffer