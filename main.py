import pandas as pd
from datetime import datetime

# Đọc file CSV
df = pd.read_csv('/home/antd/coder/projects/github/Local_Test/dataset/datamart/OrderHeader.csv')

# Hiển thị dữ liệu trước khi thay đổi
print("Dữ liệu trước khi thay đổi:")
print(df.head())
print(f"Các ngày OrderDate duy nhất trước khi thay đổi:")
print(sorted(df['OrderDate'].unique()))

# Chuyển đổi OrderDate sang datetime để so sánh
df['OrderDate'] = pd.to_datetime(df['OrderDate'])

# Tạo ngày chuẩn để so sánh: 2011-06-03
reference_date = datetime(2011, 6, 3)
reference_date_2 = datetime(2025, 10, 11)

# Tìm tất cả các ngày >= 2011-06-03 (bất kể năm nào)
condition = (df['OrderDate'] >= reference_date) & (df['OrderDate'] < reference_date_2)

# Đếm số dòng sẽ được thay đổi
count_to_change = condition.sum()
print(f"\nSố dòng có OrderDate >= 2011-06-03: {count_to_change}")

# Hiển thị một vài ví dụ về ngày sẽ được thay đổi
print("Một số ví dụ ngày sẽ được thay đổi:")
sample_dates = df[condition]['OrderDate'].unique()[:10]
for date in sorted(sample_dates):
    print(f"  {date}")

# Thay đổi tất cả các ngày >= 2011-06-03 thành 2025-10-11
df.loc[condition, 'OrderDate'] = '2025-10-11 00:00:00.000'

# Hiển thị dữ liệu sau khi thay đổi
print(f"\nDữ liệu sau khi thay đổi:")
print(df.head())
print(f"Các ngày OrderDate duy nhất sau khi thay đổi:")
print(sorted(df['OrderDate'].unique()))

# Lưu lại file CSV
df.to_csv('/home/antd/coder/projects/github/Local_Test/dataset/datamart/OrderHeader.csv', index=False)

print(f"\nĐã lưu file thành công!")
print(f"Thống kê: {count_to_change} dòng đã được thay đổi từ các ngày >= 2011-06-03 thành 2025-10-11")