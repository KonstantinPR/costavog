import pandas as pd
import numpy as np
from app.modules import pandas_handler



def abc_xyz(merged_df):
    # Calculate total margin by summing all 'Маржа-себест.' columns
    margin_columns = [col for col in merged_df.columns if 'Маржа-себест.' in col and "Маржа-себест./ шт" not in col]
    merged_df[margin_columns] = merged_df[margin_columns].applymap(pandas_handler.false_to_null)
    merged_df['Total_Margin'] = merged_df[margin_columns].sum(axis=1)
    most_total_marging = sum([x for x in merged_df['Total_Margin'] if x > 0]) / 2
    most_total_marging_loss = sum([x for x in merged_df['Total_Margin'] if x < 0]) / 2

    # Calculate total margin per article
    total_margin_per_article = merged_df.groupby('Артикул поставщика')['Total_Margin'].sum().reset_index()

    # Sort by total margin in descending order for ABC analysis
    total_margin_per_article = total_margin_per_article.sort_values(by='Total_Margin', ascending=False)
    total_margin_per_article['Cumulative_Margin'] = total_margin_per_article['Total_Margin'].cumsum()
    total_margin_per_article['Cumulative_Percentage'] = (total_margin_per_article['Cumulative_Margin'] /
                                                         total_margin_per_article['Total_Margin'].sum()) * 100

    # Classify into ABC categories

    def classify_abc(row):
        if row["Total_Margin"] > most_total_marging:
            return 'A'
        if row["Total_Margin"] > 0:
            return 'B'
        elif row["Total_Margin"] == 0:
            return 'C'
        elif row["Total_Margin"] < 0 and row["Total_Margin"] > most_total_marging_loss:
            return 'D'
        else:
            return 'E'

    total_margin_per_article['ABC_Category'] = total_margin_per_article.apply(classify_abc, axis=1)

    # Add ABC categories to merged_df
    merged_df = merged_df.merge(total_margin_per_article[['Артикул поставщика', 'ABC_Category']],
                                on='Артикул поставщика', how='left')

    # XYZ Analysis with Weighted CV
    sales_quantity_columns = [col for col in merged_df.columns if col.startswith('Ч. Продажа шт.')]

    # Define weights for sales periods
    weights = np.linspace(1, 0.5, len(sales_quantity_columns))  # Adjust this based on your needs
    weights = weights / weights.sum()  # Normalize weights

    def calculate_weighted_cv(row):
        quantities = row[sales_quantity_columns].replace('', 0).astype(
            float).values  # Convert empty strings to NaN and then to float
        if np.all(pd.isna(quantities)):  # Handle all NaN case
            return float('inf')  # Handle division by zero if all quantities are NaN
        weighted_mean = np.average(quantities, weights=weights, returned=False)
        weighted_std_dev = np.sqrt(np.average((quantities - weighted_mean) ** 2, weights=weights))
        if weighted_mean == 0:
            return float('inf')  # Handle division by zero if all quantities are zero
        return weighted_std_dev / weighted_mean

    merged_df['CV'] = merged_df.apply(calculate_weighted_cv, axis=1)

    # Convert CV to numeric, coercing errors to NaN
    merged_df['CV'] = pd.to_numeric(merged_df['CV'], errors='coerce')

    # Create CV_mod column with absolute values, preserving NaN for non-numeric entries
    merged_df['CV_mod'] = merged_df['CV'].apply(lambda x: abs(x) if pd.notna(x) else x)

    # Classify into XYZ Categories
    periods = len(sales_quantity_columns)

    def classify_xyz(row):
        cv = row['CV']
        if cv <= periods / 8 and cv > 0:
            return 'W'
        elif cv <= periods / 4 and cv > 0:
            return 'X'
        elif cv <= periods and cv > 0:
            return 'Y'
        else:
            return 'Z'

    merged_df['XYZ_Category'] = merged_df.apply(classify_xyz, axis=1)

    # Ensure no empty values in Total_Margin and CV, fill them with 0
    merged_df["Total_Margin"].replace(["", np.inf, -np.inf], 0, inplace=True)
    merged_df["CV"].replace(["", np.inf, -np.inf], 0, inplace=True)

    # In case we want to ensure CV has a minimum non-zero value for comparison
    max_cv = merged_df["CV"].max()
    merged_df["CV"].replace(0, max_cv, inplace=True)  # Replace 0 CV with the maximum CV value if necessary
    first_columns = ["Артикул поставщика", "Total_Margin", "ABC_Category", "CV", "XYZ_Category"]
    merged_df = merged_df.reindex(
        columns=first_columns + [col for col in merged_df.columns if col not in first_columns])

    return merged_df
