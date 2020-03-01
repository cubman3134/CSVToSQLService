from time import sleep
import numpy as np
import pandas as pd
import os


def pStr(curStr):
    if(str(curStr).lower() == "nan"):
        return ""
    return str(curStr)

def ProcessSqlFile(fileName):
    os.system("mysql parkerrr2020 < " + fileName)
    os.system("rm " + fileName)

"""
Creat a new procedure that only deals with this specific table.
"""
def CreateLocalProc(curFilePath, tableName, data):
    currentNumber = 0
    while True:
        procName = curFilePath + tableName + "proc" + currentNumber 
        if os.path.isfile(procName):
            currentNumber += 1
            continue
        f = open(procName, "w")

        f.close()

"""
Create a table with the given tableName and the given
definition, type, description1, description2, description3
in data. Automatically sets the primary key from description3 column.
"""
def CreateTable(curFilePath, tableName, data):
    f = open(curFilePath + tableName + ".sql", "w") 
    f.write("DROP TABLE IF EXISTS " + tableName + ";\n")
    f.write("CREATE TABLE " + tableName + " (\n")
    PrimaryKeys = []
    for index, row in data.iterrows():
        f.write(pStr(row['DEFINITION']) + " " + pStr(row['TYPE']) + " " + pStr(row['DESCRIPTION1']) + " " + pStr(row['DESCRIPTION2']) + ",\n")
        if(row['DESCRIPTION3'] == "PRIMARY KEY"):
            PrimaryKeys.append(row['DEFINITION']) 
        if index + 1 == len(data):
            f.write("CONSTRAINT PK_" + tableName + " PRIMARY KEY (")
            for curPrimaryKeyIndex in range(len(PrimaryKeys)):
                f.write(PrimaryKeys[curPrimaryKeyIndex])
                if curPrimaryKeyIndex + 1 != len(PrimaryKeys):
                    f.write(",")
            f.write(")\n")  
        #print(row['DEFINITION'], row['TYPE'], row['DESCRIPTION'])
    #f.write("ALTER TABLE " + tableName + " ADD CONSTRAINT PK_" + tableName + " PRIMARY KEY (")
    
    f.write(");\n")
    f.close()
    print(data)

def TakeApartCSV(filePath):
    tableName = filePath.split('/')[-1][:-4]
    curFilePath = filePath.replace(filePath.split('/')[-1], "")
    df = pd.read_csv(filePath)
    CreateTable(curFilePath, tableName, df[['DEFINITION', 'TYPE', 'DESCRIPTION1', 'DESCRIPTION2', 'DESCRIPTION3']])


def main():
    timeToWaitMillis = 50
    sqlFilesLocation = "/home/ec2-user/ToDoMySql"
    filesCurrentlyExist = False
    while True:
        sleep(timeToWaitMillis * 0.001)
        filesCurrentlyExist = False
        for file in os.listdir(sqlFilesLocation):
            filesCurrentlyExist = True
            if file.endswith(".csv"):
                TakeApartCSV(os.path.join(sqlFilesLocation, file))
            if file.endswith(".sql"):
                ProcessSqlFile(os.path.join(sqlFilesLocation, file))
        if filesCurrentlyExist == False:
            timeToWaitMillis = 500
        else:
            timeToWaitMillis = 50

if __name__ == "__main__":
    main()
