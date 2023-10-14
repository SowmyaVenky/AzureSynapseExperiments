## Azure Data Lake Security Ideas

* This page details out various options we can use to organize the data in a data lake and secure it in many ways using Azure's RBAC, ABAC and ACL. 

* The highest grain of access control available for us is the Role based access control. We can create custom role names and add different permissions to it. Once that role is created, we can apply that role to the resource we want to control. 

<img src="./images/adls_sec_001.png" />

### Storage account for each SOR

* The idea here is to create a separate storage account for each SOR we are trying to bring into the data lake. This might seem like an overkill at a first glance. This might be the only way to get things like:
    * Separate encryption key for each SOR. 
    * Full flexibility on where the data for the SOR can be placed. Like for instance a particular SOR might need to keep its data in the EU region. If that is the case, we need to make sure we have a storage account created at that region. 
    * Complete isolation on the roles created and managed. The SOR owner can create and maintain the groups/users needed. 

<img src="./images/adls_sec_002.png" />
