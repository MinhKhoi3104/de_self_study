--Thực hiện giả lập dữ liệu day 2
-- insert
insert into e_commerce.customer(customer_id,full_name,date_of_birth,hometown,phone_number,email, gender) 
values ('11','Nguyễn Minh Khôi',Date'2004-01-03','Đồng Tháp','0823171080', 'khoinguyendata.work@gmail.com','Nam');
-- update
-- thay hometown + gender
update e_commerce.customer
set hometown = 'Đồng Tháp' , gender = 'Gay'
where customer_id = '1';

-- thay phonenum + gender
update e_commerce.customer
set phone_number = '8888888888' , gender = 'Les'
where customer_id = '2';

-- Thay date_of_birth
update e_commerce.customer
set date_of_birth = '2012-01-03'
where customer_id = '3';

--delete
delete from e_commerce.customer
where customer_id = '4'