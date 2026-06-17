Xây dựng hệ thống đếm số lượng người hiện diện trong một camera, hệ thống bao gồm:

Một server nhận các khung hình từ camera và gửi các khung hình đó đến cho server xử lý.
Một server xử lý thực thi quá trình nhận diện đối tượng có trong hình ảnh. Server này tiến hành xử lý khung hình và trả về tập hợp các khung đóng (bounding box) chỉ vị trí của các đối tượng có trong hình ảnh và gửi kết quả này đến server lưu trữ.
Một server lưu trữ kết quả.
Toàn bộ mã nguồn và kết quả trả về được commit lên github. Sinh viên sử dụng link Github để nộp bài thực hành. Lưu ý phải xây dựng hệ thống trong ngữ cảnh có sử dụng công nghệ dữ liệu lớn.