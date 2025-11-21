# PYSPARK_ASSIGNMENT

# 1. Question: 

## 1.1.Create DataFrame as purchase_data_df,  product_data_df with custom schema with the below data 

<img width="1640" height="618" alt="image" src="https://github.com/user-attachments/assets/8060bf80-00ca-4045-98a8-fa385b0b4fb5" />

<img width="1605" height="267" alt="image" src="https://github.com/user-attachments/assets/a19fbaa0-8e0b-4ccc-a185-fbd6d8289772" />

## output:

<img width="1648" height="562" alt="image" src="https://github.com/user-attachments/assets/7700cf4b-9296-40c8-a5ee-4155e8838252" />

## 1.2.Find the customers who have bought only iphone13 

<img width="1481" height="117" alt="image" src="https://github.com/user-attachments/assets/314c520c-bf9c-4892-9c0a-25accb598858" />

## output:

<img width="1626" height="124" alt="image" src="https://github.com/user-attachments/assets/f7cf4d4c-000d-43a2-a7c5-2dd0235e9977" />

## 1.3.Find customers who upgraded from product iphone13 to product iphone14 

<img width="1486" height="131" alt="image" src="https://github.com/user-attachments/assets/889af507-9d05-4160-9e0d-411589a6ac55" />


## output:

<img width="1632" height="143" alt="image" src="https://github.com/user-attachments/assets/220668ee-3e3a-4f5f-a2f6-a412a1cd0250" />

## 1.4.Find customers who have bought all models in the new Product Data 

<img width="1434" height="33" alt="image" src="https://github.com/user-attachments/assets/375fe4e6-f252-4f09-a2d1-75705bf6d4f8" />

<img width="1423" height="111" alt="image" src="https://github.com/user-attachments/assets/1d82e545-1630-4899-b234-cd344326ad0e" />

## output:

<img width="1631" height="120" alt="image" src="https://github.com/user-attachments/assets/55df35c8-bebc-46bf-b683-ff1f26cb4209" />

# 2.Question: 

### DataSet:Column(“card_number”) 
```
("1234567891234567",), 
 ("5678912345671234",), 
 ("9123456712345678",), 
 ("1234567812341122",), 
 ("1234567812341342",)
```
<img width="1499" height="289" alt="image" src="https://github.com/user-attachments/assets/17738fd0-998c-4f53-9da6-258df586f96d" />


## 2.1. print number of partitions 
<img width="1465" height="337" alt="image" src="https://github.com/user-attachments/assets/bd3e9599-d441-4ee8-8adb-5ce65e9f7a12" />

## output:
<img width="1622" height="49" alt="image" src="https://github.com/user-attachments/assets/a6950cc9-2162-4150-a09f-fbc586942a9c" />

## 2.2. set the partition size to 5 
<img width="1615" height="113" alt="image" src="https://github.com/user-attachments/assets/3ac988cc-6ee5-40d7-87a8-02efcda3922d" />
## output:
<img width="1622" height="92" alt="image" src="https://github.com/user-attachments/assets/3ce9c41c-116d-4939-9b87-167a7d6a0112" />

## 2.3. Decrease the partition to 3: 

<img width="1572" height="92" alt="image" src="https://github.com/user-attachments/assets/7b46ee27-4b6e-4ec7-881f-9e4ca7cd0396" />

## output:
<img width="1605" height="56" alt="image" src="https://github.com/user-attachments/assets/31b26230-6e8d-49b6-b48d-80810e333e81" />

## 2.4.Create a UDF to print only the last 4 digits marking the remaining digits as * 

Eg: ************4567 
<img width="1578" height="125" alt="image" src="https://github.com/user-attachments/assets/034d9138-1d0b-4aee-ad5d-abc88384edf9" />

## 2.5.output should have 2 columns as card_number, masked_card_number:
<img width="1578" height="125" alt="image" src="https://github.com/user-attachments/assets/910f1a68-457a-4999-9617-82397032f1ec" />

## output:
<img width="1556" height="205" alt="image" src="https://github.com/user-attachments/assets/b10e74be-bda8-4bb6-8c31-3d82a5653cea" />


#  3. Question: 

## 3.1. Create a Data Frame with custom schema creation by using Struct Type and Struct Field 
```
data = [ 
 (1, 101, 'login', '2023-09-05 08:30:00'), 
 (2, 102, 'click', '2023-09-06 12:45:00'), 
 (3, 101, 'click', '2023-09-07 14:15:00'), 
 (4, 103, 'login', '2023-09-08 09:00:00'), 
 (5, 102, 'logout', '2023-09-09 17:30:00'), 
 (6, 101, 'click', '2023-09-10 11:20:00'), 
 (7, 103, 'click', '2023-09-11 10:15:00'), 
 (8, 102, 'click', '2023-09-12 13:10:00') 
] 
```
### Column names: log id, user$id, action, and timestamp. 
<img width="1405" height="666" alt="image" src="https://github.com/user-attachments/assets/b76fb3a5-d8fa-47bb-8243-14f4684a77be" />

<img width="1622" height="268" alt="image" src="https://github.com/user-attachments/assets/625ee178-bada-4697-b8ab-5394519d00d2" />


## 3.2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function 
<img width="1417" height="202" alt="image" src="https://github.com/user-attachments/assets/544a6731-5d27-41e2-871c-090c9ff4770d" />

## Output:
<img width="1551" height="268" alt="image" src="https://github.com/user-attachments/assets/91afdcd7-a4b5-48a0-9a89-1f4c53e788f0" />

## 3.3. Write a query to calculate the number of actions performed by each user in the last 7 days 
<img width="1432" height="229" alt="image" src="https://github.com/user-attachments/assets/5aa2eff4-61e7-4055-aa1b-b7b2d1175a67" />


## Output:
<img width="1505" height="175" alt="image" src="https://github.com/user-attachments/assets/20a140e0-8b35-4665-8706-48e76f4d4bd7" />


## 3.4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type 
<img width="1481" height="47" alt="image" src="https://github.com/user-attachments/assets/465f13af-eb83-4afe-be19-cbde59c0d41f" />

## Output:

<img width="1489" height="246" alt="image" src="https://github.com/user-attachments/assets/c19fcf3a-9e8e-4cb0-84d5-182722a7d79d" />

# 4. Question 

## 4.1. Read JSON file provided in the attachment using the dynamic function 
<img width="1437" height="379" alt="image" src="https://github.com/user-attachments/assets/aed50710-9471-4a9f-a5fb-5c887f1e8692" />

## Output:

<img width="1597" height="107" alt="image" src="https://github.com/user-attachments/assets/4b11a3e0-7474-49cf-95bf-1ddb6f8eee94" />

## 4.2. flatten the data frame which is a custom schema 
<img width="1267" height="275" alt="image" src="https://github.com/user-attachments/assets/a38db274-33b2-499f-8359-f2a0a58a40b2" />

## Output:

<img width="1219" height="164" alt="image" src="https://github.com/user-attachments/assets/30ba3668-e851-43cc-b702-508884118ad1" />

## 4.3. find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count) 
<img width="1447" height="152" alt="image" src="https://github.com/user-attachments/assets/b29d0a43-f7fa-4dd3-9fe0-ed460ab438bf" />

## Output:

<img width="1231" height="55" alt="image" src="https://github.com/user-attachments/assets/ddb9a01b-96a0-4b59-8d03-86226903864a" />


## 4.4. Differentiate the difference using explode, explode outerfunctions 

<img width="1356" height="75" alt="image" src="https://github.com/user-attachments/assets/7f2ddf96-6900-4e79-b58b-04830672e07e" />

## Output:

<img width="1636" height="332" alt="image" src="https://github.com/user-attachments/assets/aac7aa12-0b74-4b3f-bc67-40a608827a0d" />



## 4.5. Filter the id which is equal to 0001  

<img width="1380" height="67" alt="image" src="https://github.com/user-attachments/assets/003848ef-84a7-479b-8262-740c74525e56" />

## Output:
 <img width="1363" height="117" alt="image" src="https://github.com/user-attachments/assets/63afcc7c-bab5-4a80-a3b9-5c4ba3039f16" />



## 4.6. Add a new column named load_date with the current date 

<img width="1229" height="94" alt="image" src="https://github.com/user-attachments/assets/2064b8ae-8eb6-4a2d-a04c-3c6828fc9d05" />

## Output:
 <img width="1316" height="162" alt="image" src="https://github.com/user-attachments/assets/a02e323b-208b-4e99-8356-09027ebd09b4" />


## 4.7. create 3 new columns as year, month, and day from the load_date column 
<img width="1345" height="310" alt="image" src="https://github.com/user-attachments/assets/a4b7a9b7-6dcf-4c58-8d04-a6247faa7927" />


## Output:

<img width="1616" height="182" alt="image" src="https://github.com/user-attachments/assets/49bb0ec2-4f6a-4362-8243-6656d5bda1c0" />



# 5. Question 

## DataSet1:Column names(employee id, employee_name, department, State, salary, Age) 
```
((11,“james”,” D101”,”ny”,9000,34)), 

(12,”michel”,” D101”,”ny”,8900,32), 

(13,“robert”,” D102”,”ca”,7900,29), 

(14,“scott”,” D103”,”ca”,8000,36), 

(15,“jen”,” D102”,”ny”,9500,38), 

(16,”jeff”,” D103”,”uk”,9100,35), 

(17,“maria”,” D101”,”ny”,7900,40)) 

 ```

## Dataset2:Column names(dept_id, dept_name) 
```
((“D101”,”sales”), 

(“D102”,”finance”), 

(”D103”,”marketing”), 

(“D104”,”hr”), 

(“D105”,”support”)) 

 ```

## Dataset3: Column names(country_code, country_name) 
```
((“ny”,”newyork”), 

(“ca”,”California”), 

(“uk”,”Russia)) 

 ```

## 5.1. create all 3 data frames as employee_df, department_df, country_df with custom schema defined in dynamic way 
<img width="1458" height="628" alt="image" src="https://github.com/user-attachments/assets/5b9ae292-5134-4c8a-a498-36c2b2234fb5" />
<img width="1332" height="726" alt="image" src="https://github.com/user-attachments/assets/a08d2820-72ee-4969-a5cd-9ebae714645b" />

<img width="1036" height="240" alt="image" src="https://github.com/user-attachments/assets/c0cf29f6-e7e8-4d38-b667-f6e781b84ca4" />
<img width="1258" height="209" alt="image" src="https://github.com/user-attachments/assets/0359911f-89be-41b7-b00a-9955b4655ed7" />
<img width="1000" height="180" alt="image" src="https://github.com/user-attachments/assets/52b4507b-68c3-4b4a-8750-c66934eb195b" />


## 5.2. Find avg salary of each department 
<img width="1285" height="95" alt="image" src="https://github.com/user-attachments/assets/84345a74-8392-4f69-abd2-003c3ef80f88" />

## Output:
<img width="1081" height="185" alt="image" src="https://github.com/user-attachments/assets/4d8beecc-eb1f-42ef-bfc4-7a05bc3ac827" />

## 5.3. Find the employee’s name and department name whose name starts with ‘m’  
<img width="1428" height="92" alt="image" src="https://github.com/user-attachments/assets/9ac65107-2812-41aa-8b2d-70a38d83ae13" />

## Output:
<img width="1266" height="147" alt="image" src="https://github.com/user-attachments/assets/c29fdfc1-bbb6-4f73-b666-39c59c02d3d3" />

## 5.4. Create another new column in  employee_df as a bonus by multiplying employee salary *2 
<img width="1090" height="69" alt="image" src="https://github.com/user-attachments/assets/9915d0c6-7e4d-48cd-99e9-dfbd6ee19d2e" />


## Output:
<img width="1292" height="251" alt="image" src="https://github.com/user-attachments/assets/a8165151-4b39-41e5-b172-e393c587c90d" />

## 5.5. Reorder the column names of employee_df columns as (employee_id,employee_name,salary,State,Age,department) 

<img width="1316" height="184" alt="image" src="https://github.com/user-attachments/assets/d776f127-1520-4151-8889-2f7e16c9f7c4" />

## Output:
<img width="1085" height="254" alt="image" src="https://github.com/user-attachments/assets/61f94ec7-9857-4d9b-a6b5-68262b5661dd" />

## 5.6. Give the result of an inner join, left join, and right join when joining employee_df with department_df in a dynamic way 
### innerjoin:
<img width="1245" height="110" alt="image" src="https://github.com/user-attachments/assets/bbb3e234-efc4-43c4-a72b-54b9d0ada045" />


### leftjoin:
<img width="1357" height="113" alt="image" src="https://github.com/user-attachments/assets/8cfffff5-bfde-4908-8f6b-0c9f80910834" />

### rightjoin:

<img width="1287" height="112" alt="image" src="https://github.com/user-attachments/assets/8da7142b-21fd-4ded-a1e3-b7daa4d63378" />

## Output:

<img width="1306" height="251" alt="image" src="https://github.com/user-attachments/assets/ec410df9-c1dd-42b8-8e53-88e6305e4413" />

<img width="1337" height="568" alt="image" src="https://github.com/user-attachments/assets/b5756c36-748d-41b5-bc5d-5b9f30842219" />

## 5.7. Derive a new data frame with country_name instead of State in employee_df  
Eg(11,“james”,”D101”,”newyork”,8900,32) 

<img width="1276" height="247" alt="image" src="https://github.com/user-attachments/assets/7b5331c6-47c6-4b20-8afc-6c24323bb313" />


## Output:
<img width="1414" height="259" alt="image" src="https://github.com/user-attachments/assets/1daac272-f99c-4eff-b923-a40d8d20526c" />

## 5.8. convert all the column names into lowercase from the result of question 7in a dynamic way, add the load_date column with the current date 

<img width="1426" height="175" alt="image" src="https://github.com/user-attachments/assets/49caf668-d747-40ea-beff-334f5b2f8974" />


## Output:

<img width="1466" height="255" alt="image" src="https://github.com/user-attachments/assets/856f9049-ae77-4553-8125-02c44a768df5" />












