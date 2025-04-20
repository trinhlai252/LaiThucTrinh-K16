
#!/bin/bash


# 1. Sort theo relaese_date


# Bước 1: Loại dòng lỗi dấu ngoặc kép không đóng
grep -E '^([^"]*("[^"]*")?)*$' tmdb-movies.csv > movies-clean.csv

# Bước 2: Thay dấu ',' trong dấu ngoặc kép thành ';'
awk '{
  n = split($0, a, "");
  in_quotes = 0;
  for (i = 1; i <= n; i++) {
    if (a[i] == "\"") in_quotes = !in_quotes;
    else if (in_quotes && a[i] == ",") a[i] = ";";
  }
  for (i = 1; i <= n; i++) printf "%s", a[i];
  print "";
}' movies-clean.csv > temp && mv temp movies-clean.csv

# Bước 3: Loại dòng có số cột khác 21
awk -F',' 'NF == 21' movies-clean.csv > temp && mv temp movies-clean.csv

# Bước 4: Loại dòng có cột 16 rỗng
awk -F',' 'length($16) > 0' movies-clean.csv > temp && mv temp movies-clean.csv

# Bước 5: Chuẩn hóa release_date (cột 16) theo release_year (cột 19)
awk -F',' 'BEGIN {OFS=","}
NR == 1 {print; next}
{
  gsub(/^ +| +$/, "", $16);
  gsub(/^ +| +$/, "", $19);
  split($19, year_parts, /"/);
  real_year = year_parts[1];
  split($16, d, "/");
  if (length(d) == 3) {
    m = (length(d[1]) == 1 ? "0"d[1] : d[1]);
    day = (length(d[2]) == 1 ? "0"d[2] : d[2]);
    $16 = real_year "-" m "-" day;
    print
  }
  else if (length(d) == 2 && real_year ~ /^[0-9]{4}$/) {
    m = (length(d[1]) == 1 ? "0"d[1] : d[1]);
    day = (length(d[2]) == 1 ? "0"d[2] : d[2]);
    $16 = real_year "-" m "-" day;
    print
  }
}' movies-clean.csv > movies-clean-date.csv

# Bước 6: Sắp xếp theo release_date (cột 16) giảm dần
csvsort -c 16 -r -d ',' movies-clean-date.csv > movies-sorted.csv



# 2. Câu hỏi: Lọc ra các bộ phim có đánh giá trung bình trên 7.5 rồi lưu ra một file mới
###########################################
awk -F',' 'NR==1 || $18+0 > 7.5' movies-sorted.csv > movies-over-7.5.csv



# 3. Câu hỏi: Phim có doanh thu cao nhất và thấp nhất

tail -n +2 movies-sorted.csv | awk -F',' '$21+0 > 0' | sort -t',' -k21,21nr | head -n 1 | awk -F',' '{print "Highest:", $6, $21}' >> revenue.txt
tail -n +2 movies-sorted.csv | awk -F',' '$21+0 > 0' | sort -t',' -k21,21n  | head -n 1 | awk -F',' '{print "Lowest:", $6, $21}'  >> revenue.txt


# 4. Câu hỏi: Top 10 bộ phim đem lại lợi nhuận cao nhất

tail -n +2 movies-sorted.csv | awk -F',' '{profit = $21 - $20; print profit "," $6}' | sort -t',' -k1,1nr | head -n 10 > top-profit-movies.txt



# 5. Câu hỏi: Đạo diễn và diễn viên nào nhiều phim nhất

cut -d',' -f9 movies-sorted.csv | tail -n +2 | tr '|' '\n' | sort | uniq -c | sort -nr | head -n 1 >> people-summary.txt
cut -d',' -f7 movies-sorted.csv | tail -n +2 | tr '|' '\n' | grep -v '^$' | sort | uniq -c | sort -nr | head -n 1 >> people-summary.txt



# 6. Câu hỏi: Thống kê số lượng phim theo từng thể loại

cut -d',' -f14 movies-sorted.csv | tail -n +2 | tr '|' '\n' | grep -v '^$' | sort | uniq -c | sort -nr > genre-count.txt


#####################################
# 7. Ý tưởng mở rộng để phân tích dữ liệu

# - Phân tích theo thể loại (genre): 
#   + Thể loại phổ biến nhất
#   + Doanh thu trung bình / Lợi nhuận trung bình / Điểm đánh giá trung bình theo thể loại
#   + Xu hướng làm phim theo thể loại qua các năm

# - Phân tích theo thời gian:
#   + Số lượng phim theo từng năm
#   + Quý hoặc tháng nào có nhiều phim phát hành nhất

# - Phân tích đạo diễn và diễn viên:
#   + Đạo diễn/diễn viên nào có doanh thu trung bình cao nhất
#   + Phim của họ thường thuộc thể loại nào?
#   + Họ có ảnh hưởng tới điểm đánh giá hay không?

# - Phân tích mối liên hệ:
#   + Giữa điểm đánh giá và doanh thu
#   + Giữa ngân sách và lợi nhuận
#   + Giữa vote_count và thành công thương mại

