import pandas as pd

# Creating the first DataFrame df1
data1 = {
    'A': [1, 4],
    'B': [3, 2],
    'C': [4, 1],
    'E': [5, 9]  # Additional data in the E column for demonstration
}
df1 = pd.DataFrame(data1)

# Creating the second DataFrame df2 with predefined headers
data2 = {
    'B': ['B', 'Z'],
    'N': ['C', 'F'],
    'S': ['E', 'A'],
    'K': ['L', 'M']
}
df2 = pd.DataFrame(data2)
print(df2)
# Perform the merge. Note that 'how' can be adjusted as needed (left, right, inner)


merged_df = df2.merge(df1, how='left', left_on='B', right_on='B')

# Display the resulting DataFrame
print(merged_df.reset_index(drop=True))
