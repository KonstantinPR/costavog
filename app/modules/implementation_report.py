import re
import pdfplumber
from datetime import datetime


# Define default patterns
default_pattern = r".*?(\d{1,9}(?:\.\d{2})*,\d{2})"

# Define patterns for extracting financial data
patterns = {
    "total_product_value": r"Всего стоимость реализованного товара" + default_pattern,
    "total_deducted_value": r"Итого зачтено из стоимости реализованного товара" + default_pattern,
    "wildberries_reward": r"Сумма вознаграждения Вайлдберриз" + default_pattern,
    "vat": r"НДС с вознаграждения Вайлдберриз" + default_pattern,
    "international_shipping_cost": r"Стоимость услуг Вайлдберриз по организации международной перевозки" + default_pattern,
    "acquiring_costs": r"Возмещение издержек по эквайрингу" + default_pattern,
    "agent_expenses": r"Возмещение расходов поверенного" + default_pattern,
    "penalties": r"Штрафы" + default_pattern,
    "other_deductions": r"Прочие удержания" + default_pattern,
    "compensation_damage": r"Компенсация ущерба" + default_pattern,
    "other_payments": r"Прочие выплаты" + default_pattern,
    "final_amount": r"Итого к перечислению Продавцу за текущий период" + default_pattern
}

# Define the desired column names
column_names = {
    "total_product_value": "Всего стоимость реализованного товара",
    "total_deducted_value": "Итого зачтено из стоимости реализованного товара",
    "wildberries_reward": "Сумма вознаграждения Вайлдберриз",
    "vat": "НДС с вознаграждения Вайлдберриз",
    "international_shipping_cost": "Стоимость услуг Вайлдберриз по организации международной перевозки",
    "acquiring_costs": "Возмещение издержек по эквайрингу",
    "agent_expenses": "Возмещение расходов поверенного",
    "penalties": "Штрафы",
    "other_deductions": "Прочие удержания",
    "compensation_damage": "Компенсация ущерба",
    "other_payments": "Прочие выплаты",
    "final_amount": "Итого к перечислению Продавцу за текущий период"
}

def extract_text_from_pdf(pdf_file):
    """Extract text from a PDF file."""
    try:
        with pdfplumber.open(pdf_file) as pdf:
            text = ''.join(page.extract_text() or '' for page in pdf.pages)
    except Exception as e:
        raise RuntimeError(f"Failed to process PDF file: {str(e)}")
    return text

def extract_financial_data(text):
    """Extract financial data from text using regex patterns."""
    extracted_data = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.DOTALL)
        if match:
            value = float(match.group(1).replace(',', '.'))
            extracted_data[column_names[key]] = value
        else:
            extracted_data[column_names[key]] = None
    return extracted_data

def extract_additional_info(text, file_name):
    """Extract additional information like file name, document number, and period."""
    extracted_data = {"Имя файла": file_name}

    doc_number_pattern = r"Отчет Вайлдберриз[а-яА-Яa-zA-Z\s]*.\s*(\d+)\s*от\s*"
    doc_number_match = re.search(doc_number_pattern, text)
    extracted_data["Номер документа"] = doc_number_match.group(1) if doc_number_match else None

    period_pattern = r"за период с (\d{4}-\d{2}-\d{2}) по (\d{4}-\d{2}-\d{2})"
    period_match = re.search(period_pattern, text)
    if period_match:
        start_date_str, end_date_str = period_match.groups()
        extracted_data["Дата начала"] = start_date_str
        extracted_data["Дата окончания"] = end_date_str
    else:
        extracted_data["Дата начала"] = None
        extracted_data["Дата окончания"] = None

    return extracted_data

def calculate_financial_totals(extracted_data, text):
    """Calculate additional financial totals."""
    extracted_data["Добавить доход в бухгалтерию"] = extracted_data[column_names["total_product_value"]] - \
                                                     extracted_data[column_names["final_amount"]] + \
                                                     extracted_data[column_names["compensation_damage"]] + \
                                                     extracted_data[column_names["other_payments"]]

    total_deducted_value = extracted_data.get("Итого зачтено из стоимости реализованного товара", 0) or 0
    penalties = extracted_data.get("Штрафы", 0) or 0
    other_deductions = extracted_data.get("Прочие удержания", 0) or 0
    compensation_damage = extracted_data.get("Компенсация ущерба", 0) or 0
    extracted_data["Добавить расход в бухгалтерию"] = total_deducted_value - penalties

    date_pattern = r"за период с (\d{4}-\d{2}-\d{2}) по (\d{4}-\d{2}-\d{2})"
    date_match = re.search(date_pattern, text)
    if date_match:
        start_date_str = date_match.group(1)
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        quarter = (start_date.month - 1) // 3 + 1
        extracted_data['Квартал'] = f'Q{quarter}'
    else:
        extracted_data['Квартал'] = None

    return extracted_data

def process_pdf_file(pdf_file):
    """Process a single PDF file and return extracted data."""
    text = extract_text_from_pdf(pdf_file)
    extracted_data = extract_financial_data(text)
    additional_info = extract_additional_info(text, pdf_file.filename)
    extracted_data.update(additional_info)
    extracted_data = calculate_financial_totals(extracted_data, text)
    return extracted_data

def pdf_files_process(pdf_files):
    all_data = []
    for pdf_file in pdf_files:
        try:
            # Process the PDF file and extract data
            extracted_data = process_pdf_file(pdf_file)
            all_data.append(extracted_data)
        except Exception as e:
            return f"Failed to process PDF file: {str(e)}", 500
    return all_data

def totals_calculate(df):
    df.loc['Total'] = df.sum(numeric_only=True)
    df_totals = df.groupby('Квартал').sum(numeric_only=True).reset_index()
    df_totals['Квартал'] = df_totals['Квартал'].astype(str)
    return df_totals

def grand_calculate(df):
    grand_totals = df.sum(numeric_only=True).to_frame().T
    grand_totals['Квартал'] = 'Total'
    return grand_totals
