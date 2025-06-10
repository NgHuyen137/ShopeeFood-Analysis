import csv

with open('./data/delivery_info_clean.csv', 'r', encoding='utf-8') as infile, \
     open('./data/temp_clean.csv', 'w', encoding='utf-8', newline='') as outfile:
    reader = csv.DictReader(infile)
    writer = csv.DictWriter(outfile, fieldnames=['restaurant_id', 'avg_price'])

    writer.writeheader()  # Ghi tiêu đề
    for row in reader:
        writer.writerow({'restaurant_id': row['restaurant_id'], 'avg_price': row['avg_price']})
