from fpdf import FPDF


def images_into_pdf_2(df, art_col_name="Артикул товара", size_col_name="Размер", qt_col_name="Кол-во"):
    path_pdf = "folder_img/output.pdf"

    pdf = FPDF(orientation='P', unit='mm', format='A4')

    step = 10
    sheet_height = 297
    sheet_width = 210
    pdf.set_font('arial', 'B', 24)
    pdf.set_text_color(0, 0, 0)

    for idx, art_set in enumerate(set(df[art_col_name])):
        pdf.add_page()
        pdf.image(f"folder_img/{art_set}-1.JPG", x=0, y=0, w=sheet_width, h=sheet_height)
        txt = f"{art_set}"
        new_y = step
        pdf.add_page()
        pdf.cell(0, step, txt, border=0)
        for jdx, art_df in enumerate(df[art_col_name]):
            qt = df[qt_col_name][jdx]
            if art_df == art_set and qt:
                size = df[size_col_name][jdx]
                info = f"{size} - {qt}"
                new_y = new_y + 10
                pdf.set_xy(x=step, y=new_y)
                pdf.cell(0, step * 2, info, border=0)

    pdf.output(path_pdf)

    return path_pdf
