from fpdf import FPDF
import os.path
import os


def images_into_pdf(df, file_name="output_filename", art_col_name="Артикул товара", size_col_name="Размер",
                    qt_col_name="Кол-во", rev='Прибыль_sum', qt_sum=0):
    file_name = file_name.split('.')[0]
    path_pdf = f"folder_img/{file_name}.pdf"

    pdf = FPDF(orientation='P', unit='mm', format='A4')

    step = 10
    sheet_height = 297
    sheet_width = 210
    pdf.set_font('arial', 'B', 24)
    pdf.set_text_color(0, 0, 0)
    no_photo_list = []

    unique_values = df[art_col_name].unique()

    for art_set in unique_values:
        total_qty = df.loc[df[art_col_name] == art_set, qt_col_name].sum()

        if total_qty == 0:
            print(f"Skipping {art_set} - total quantity is 0.")
            continue

        has_photo = _add_image_to_pdf(pdf, art_set, sheet_width, sheet_height)
        if not has_photo:
            no_photo_list.append(art_set)

        _add_text_to_pdf(pdf, art_col_name, art_set, df, size_col_name, qt_col_name, step)

    _add_notes_and_info(pdf, step, no_photo_list, qt_sum)

    pdf.output(path_pdf)
    print('images_into_pdf is completed')

    return path_pdf, no_photo_list


def _add_image_to_pdf(pdf, art_set, sheet_width, sheet_height):
    if os.path.isfile(f"folder_img/{art_set}-1.JPG"):
        pdf.add_page()
        pdf.image(f"folder_img/{art_set}-1.JPG", x=0, y=0, w=sheet_width, h=sheet_height)
        return True
    else:
        print(f"No photo for {art_set}")
        return False


def _add_text_to_pdf(pdf, art_col_name, art_set, df, size_col_name, qt_col_name, step):
    more = ""
    txt = f"{art_set}{more}"
    pdf.add_page()
    pdf.cell(0, step, txt, border=0)

    for jdx, art_df in enumerate(df[art_col_name]):
        qt = df[qt_col_name][jdx]
        if art_df == art_set and qt:
            size = df[size_col_name][jdx]
            info = f"{size} - {qt}"
            pdf.set_xy(x=step, y=pdf.get_y() + 10)
            pdf.cell(0, step * 2, info, border=0)


def _add_notes_and_info(pdf, step, no_photo_list, qt_sum):
    pdf.set_font('arial', 'B', 12)
    pdf.set_text_color(0, 0, 0)
    pdf.set_xy(x=step, y=pdf.get_y() + step * 2)
    note = "* Can be produced more (* 1 - one more, * * 2 - two more ... etc)"
    pdf.cell(0, step * 2, note, border=0)

    no_photo_str = ', '.join(str(x) for x in no_photo_list)

    if no_photo_list:
        no_photo_info = f", No_photo_for_art: {no_photo_str}"
    else:
        no_photo_info = ""

    base_info = f"All: {qt_sum}{no_photo_info}"
    pdf.set_xy(x=step, y=pdf.get_y() + step * 4)
    pdf.cell(0, step * 2, base_info, border=0)
