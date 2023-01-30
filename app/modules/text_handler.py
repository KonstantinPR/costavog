import pandas as pd



def names_multiply(df, multiply):
    multiply_number = multiply + 1
    list_of_multy = []
    for art in df['Артикул']:
        for n in range(1, multiply_number):
            art_multy = f'{art}-{n}.JPG'
            if art_multy in list_of_multy:
                n = n + multiply
                art_multy = f'{art}-{n}.JPG'
            list_of_multy.append(art_multy)
    df = pd.DataFrame(list_of_multy)
    return df
