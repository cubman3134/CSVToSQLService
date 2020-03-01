# CSVToSQLService
Built as a service that sits and waits for .csv files in the ToDoMySql folder. Once one shows up in the designated format shown here:<br>
DEFINITION      TYPE    DESCRIPTION1    DESCRIPTION2    DESCRIPTION3    aPROCEDURE<br>
AccountID       BigInt  NOT NULL        AUTO_INCREMENT  PRIMARY KEY     out<br>
FirstName       Varchar(20)     NOT NULL                        <br>
LastName        Varchar(20)     NOT NULL                        <br>
Gender  TINYINT NOT NULL                        <br>
Address1        Varchar(30)     NOT NULL                <br>
Address2        Varchar(30)     NOT NULL                        <br>
City    Varchar(30)     NOT NULL                        <br>
State   Varchar(12)     NOT NULL                        <br>
DateOfBirth     DATE                            <br>
JoinDate        DATE                            <br>
Username        Varchar(30)     NOT NULL                        in<br>
Password        Varchar(30)     NOT NULL                        in<br>
Which is generated from a google sheets document, it will insert the table into the given database along with<br>
TODO: the update/select/insert procedures<br>
TODO: and select procedures attached to other tables.<br>
In the future I will do a better job of documenting this...
