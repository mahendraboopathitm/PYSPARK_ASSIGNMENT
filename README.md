# PYSPARK_ASSIGNMENT

# 1. Question: 

## 1.Create DataFrame as purchase_data_df,  product_data_df with custom schema with the below data 

<img width="1640" height="618" alt="image" src="https://github.com/user-attachments/assets/8060bf80-00ca-4045-98a8-fa385b0b4fb5" />

<img width="1605" height="267" alt="image" src="https://github.com/user-attachments/assets/a19fbaa0-8e0b-4ccc-a185-fbd6d8289772" />

# output:

<img width="1648" height="562" alt="image" src="https://github.com/user-attachments/assets/7700cf4b-9296-40c8-a5ee-4155e8838252" />

## 2.Find the customers who have bought only iphone13 

<img width="1481" height="117" alt="image" src="https://github.com/user-attachments/assets/314c520c-bf9c-4892-9c0a-25accb598858" />

# output:

<img width="1626" height="124" alt="image" src="https://github.com/user-attachments/assets/f7cf4d4c-000d-43a2-a7c5-2dd0235e9977" />

## 3.Find customers who upgraded from product iphone13 to product iphone14 

<img width="1486" height="131" alt="image" src="https://github.com/user-attachments/assets/889af507-9d05-4160-9e0d-411589a6ac55" />


# output:

<img width="1632" height="143" alt="image" src="https://github.com/user-attachments/assets/220668ee-3e3a-4f5f-a2f6-a412a1cd0250" />

## 4.Find customers who have bought all models in the new Product Data 

<img width="1434" height="33" alt="image" src="https://github.com/user-attachments/assets/375fe4e6-f252-4f09-a2d1-75705bf6d4f8" />

<img width="1423" height="111" alt="image" src="https://github.com/user-attachments/assets/1d82e545-1630-4899-b234-cd344326ad0e" />

# output:

<img width="1631" height="120" alt="image" src="https://github.com/user-attachments/assets/55df35c8-bebc-46bf-b683-ff1f26cb4209" />

2.Question: 

# DataSet:Column(“card_number”) 
```
("1234567891234567",), 
 ("5678912345671234",), 
 ("9123456712345678",), 
 ("1234567812341122",), 
 ("1234567812341342",)
```
<img width="1499" height="289" alt="image" src="https://github.com/user-attachments/assets/17738fd0-998c-4f53-9da6-258df586f96d" />


## 1. print number of partitions 
<img width="1465" height="337" alt="image" src="https://github.com/user-attachments/assets/bd3e9599-d441-4ee8-8adb-5ce65e9f7a12" />

# output:
<img width="1622" height="49" alt="image" src="https://github.com/user-attachments/assets/a6950cc9-2162-4150-a09f-fbc586942a9c" />

## 2. set the partition size to 5 
<img width="1615" height="113" alt="image" src="https://github.com/user-attachments/assets/3ac988cc-6ee5-40d7-87a8-02efcda3922d" />
# output:
<img width="1622" height="92" alt="image" src="https://github.com/user-attachments/assets/3ce9c41c-116d-4939-9b87-167a7d6a0112" />

## 3. Decrease the partition to 3: 

<img width="1572" height="92" alt="image" src="https://github.com/user-attachments/assets/7b46ee27-4b6e-4ec7-881f-9e4ca7cd0396" />

# output:
<img width="1605" height="56" alt="image" src="https://github.com/user-attachments/assets/31b26230-6e8d-49b6-b48d-80810e333e81" />

## 4.Create a UDF to print only the last 4 digits marking the remaining digits as * 

Eg: ************4567 
<img width="1578" height="125" alt="image" src="https://github.com/user-attachments/assets/034d9138-1d0b-4aee-ad5d-abc88384edf9" />

## 5.output should have 2 columns as card_number, masked_card_number:
<img width="1578" height="125" alt="image" src="https://github.com/user-attachments/assets/910f1a68-457a-4999-9617-82397032f1ec" />

# output:
<img width="1556" height="205" alt="image" src="https://github.com/user-attachments/assets/b10e74be-bda8-4bb6-8c31-3d82a5653cea" />
