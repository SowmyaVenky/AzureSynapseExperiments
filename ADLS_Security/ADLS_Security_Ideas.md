## Azure Data Lake Security Ideas

* This page details out various options we can use to organize the data in a data lake and secure it in many ways using Azure's RBAC, ABAC and ACL. 

* The highest grain of access control available for us is the Role based access control (RBAC). We can create custom role names and add different permissions to it. Once that role is created, we can apply that role to the resource we want to control. 

<img src="./images/adls_sec_001.png" />

* <b> NOTE USER A, B and C are examples. Roles should be assigned to groups and it makes it easier to add and remove users to the group. This is the best practice. This document uses users to make the concept easier to visualize. </b>

##  RBAC CASE 1 - Storage account for each SOR

* The idea here is to create a separate storage account for each SOR we are trying to bring into the data lake. This might seem like an overkill at a first glance. This might be the only way to get things like:
    * Separate encryption key for each SOR. 
    * Full flexibility on where the data for the SOR can be placed. Like for instance a particular SOR might need to keep its data in the EU region. If that is the case, we need to make sure we have a storage account created at that region. 
    * Complete isolation on the roles created and managed. The SOR owner can create and maintain the groups/users needed. 

<img src="./images/adls_sec_002.png" />

* Following points can be observed. 
    * We have a storage account created for hosting the SOR data. As we can see under the storage account, we have created containers for each dataset that we want to bring in. Under the container we can have multiple folders to hold the data based on certain partitioning strategy like date, or region etc. As we see there are also tags assigned at the container level. Container 1 has no PII/PCI, Container 2 has just PII and container 3 has both PII and PCI. 
    * Note that we have created a bunch of roles, and we can describe what the users of each role get. Also note the same user can get multiple roles assigned to them, and that way there is full flexibility of exactly what containers/datasets the user can have read/write access for. 

<img src="./images/adls_sec_003.png" />

* The obvious advantage with this approach is that the access control can be very granular in nature and we can give the users exactly what containers they need access to. The obvious disadvantage with this approach is that there is going to be an explosion of roles.If we have 5000 containers, then we will need one role for each of these containers and maintaining that would be a big nightmare. 

## RBAC CASE 2 - One storage account for the entire data lake and containers for each SOR

* In this approach, a single storage account can be provisioned for the entire data lake storage. Each container can represent a unique SOR and folders under the container can host each dataset. The things to be aware of with this approach is that there are defined throughput rates for each storage account and if the traffic is very intense we might experience bottlenecks. Also the maximum storage we can have per storage account is around 5 PB. If we are dealing with more than this data size, then we might still follow this approach, but split the data across multiple storage accounts and some kind of documentation to tell us which SOR is on which storage account. 

<img src="./images/adls_sec_004.png" />

* As we can see, one role at the storage account level can do most of the read writes to all the containers inside the data-lake. If it is acceptable, we can use this in the ETL processes and perform most of the writes using this singular role. We could still have container specific read and write roles to assign to teams similar to the approach above. As we can see the control is at the container level and so it is an all or nothing at the SOR level.

<img src="./images/adls_sec_005.png" />

* This approach results in way lesser roles to be used. There is a read and write role needed for each SOR and that can be assigned at the container level hosting the data for the SOR. Users can however get complete access to all the folder structures under the SOR's container with this approach. This may or may not be acceptable depending on the type of data contained in the SOR. For instance, this is not an issue if every user accessing the system can see the entire dataset. This may not work in scenarios where we need to segment the data by users (for instance only access to certain folders that have PII/PCI).

* Note we have created a new storage account for each SOR
<img src="./images/rbac_000.png" />

* Under hogan storage account, we have 3 datasets. Each dataset can have multiple folders for say date. 

<img src="./images/rbac_001.png" />

* Dataset1 has no tags, Dataset2 has PII=true, and Dataset3 has PII=true and PCI=true

<img src="./images/rbac_002.png" />

<img src="./images/rbac_003.png" />


## ABAC with RBAC Case 1 

* Next we can look at how we can use ABAC in addition to the RBAC to make it easier to control access to datasets based on the conditions. Only when the conditions are met, the person will get the access, else the access is denied. This is a great feature to use when we have complex AND, OR and NOT conditions to apply as part of the decisioning process.

<img src="./images/data-lake-storage-permissions-flow.png" />

* Tags play a very big role in the conditions piece of the puzzle. The roles are applied based on the presence of certain tags and this can get pretty powerful. 

* It is important to realize that the RBAC and ABAC are evaluated BEFORE THE ACLs are evaluated. Therefore, even if ACLs block access to a specific role, they may be granted access if the RBAC or ABAC allows that. 

<img src="./images/data-lake-storage-permissions-example.png" />

* There is a nice table given in this link https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control-model It shows exactly how the ACLs can be configured and how they work when roles are present and not present. In essence, when a person does not have a role assigned to them, the ACLs get evaluated, and all the directories hosting the file need to have a X permission to allow listing, and the final file that needs to be read/written needs to have the correct bit R, W etc. enabled. The table shows is very nicely. 



