import numpy as np
from scipy.stats import linregress


def abc_xyz(df, prefix='Маржа-себест.', power=0.5, trend_name='ABC'):
    margin_cols = sorted([col for col in df.columns if col.startswith(prefix)], reverse=True)
    time_points = np.arange(1, len(margin_cols) + 1)

    slopes = []
    for idx, row in df.iterrows():
        margins = row[margin_cols].dropna().values.astype(float)
        if len(margins) == 0:
            slope = 0
        elif np.allclose(margins, margins[0]):
            slope = 0
        else:
            # Optionally, print for debugging
            slope, intercept, r_value, p_value, std_err = linregress(time_points[:len(margins)], margins)
        slopes.append(slope)

    slopes = np.array(slopes)

    max_abs_slope = np.max(np.abs(slopes))
    if max_abs_slope == 0:
        normalized_slopes = np.zeros_like(slopes)
    else:
        normalized_slopes = slopes / max_abs_slope

    # Apply power transformation to exaggerate differences
    transformed_slopes = np.sign(normalized_slopes) * (np.abs(normalized_slopes) ** power)

    df[trend_name] = transformed_slopes

    return df
