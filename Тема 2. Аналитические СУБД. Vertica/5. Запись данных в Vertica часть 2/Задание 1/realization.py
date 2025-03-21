vertica_user = 'STV2025021827'

import csv
from pathlib import Path

dataset = 'test_dataset.csv'
N = 10000 # на этот раз можете поставить даже 10 млн

with open(dataset, 'w') as csvfile:
    fwriter = csv.writer(csvfile, delimiter='|')
    for i in range(N):
        fwriter.writerow([i, 'asds'])

# эта команда напечатает абсолютный путь к файлу, скопируйте его
print(Path(dataset).resolve())

# а это пара первых строк для визуализации результата:
with open(dataset, 'r') as csvfile:
    for i in range(5):
        print(csvfile.readline(), end='')