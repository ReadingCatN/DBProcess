# Notes for Database and Python learning

## Background

 _Due to the restriction of direct access to ERP data, to better manage the business data for further analysis,_
 _to build my own database (<ins>releation database</ins>) seems to be a good way._

## Database 
### _Currently support <ins>PostgreSQL</ins> and <ins>mySQL</ins>, also exploring non-relational database like <ins>mongoDB</ins>._

## Languages
 _For postgreSQL, both VBA and python support perfect for database create, upgrade, read and delete, as well as batch insert_
 _For mySQL, VBA performs worse when it comes to batch insert, however python performs perfect_
 _This project is applying python,and python library <ins>psycopg2</ins> and <ins>pymysql</ins> is required_
 _SQL is also explored_

 ## DB Schema
 _1.PO Spend_
    * _PO Number and PO Item be the unique label;_
    * _For some goods managed by material, material code itself could be the unique label_
    * _While some goods are managed through material group, which shall also be included_
    * _company code, legal entity, BU, vendor info, item spend, item quantity are also needed for data aggragation_
    * _Specific buyers responsible for specific goods should also be known, which leads to next part_
_2.Department Responsibilities Divide_
    * _A mapping of buyers and their correspondent Purchasing Group shall be built_*
    * _A mapping of material group and buyers shall be built_
    *_For material code, ERP system often record the mapping_
_3.Performance records_
    * _Saving data of department now still relys on individual input, often recorded in a excel file in a sharepoint,
      _this could be the improve point for procurement digitalization_
    * _Saving data often be based on POs and_

## Updates of the Code
* _2025-4-27_
    _Data process for saving tracker add_
    _Add data fetch to pandas dataframe in dbClass_
    


