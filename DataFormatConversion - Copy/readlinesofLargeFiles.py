import pandas as pd

df = pd.read_csv('20230213_segments_3tc89a.tsv', sep='\t', header=None)
num_lines = df.shape[0]

print(num_lines)