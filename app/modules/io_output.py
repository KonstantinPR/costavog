from io import BytesIO
import pandas as pd


def io_output(df: pd.DataFrame) -> BytesIO:
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer)
    writer.close()
    output.seek(0)
    return output
