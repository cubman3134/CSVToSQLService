from time import sleep
import numpy as np
import pandas as pd
import gspread
import os
import wget
import sys
#from gsheets import Sheets


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
    PrimaryKeysTypes = []
    RegularCols = []
    RegularColsTypes = []
    for index, row in data.iterrows():
        curStr = pStr(row['DEFINITION']) + " " + pStr(row['TYPE']) + " " + pStr(row['DESCRIPTION1']) + " " + pStr(row['DESCRIPTION2'])
        curStr = curStr.rstrip() + ",\n"
        f.write(curStr)
        if(row['DESCRIPTION3'] == "PRIMARY KEY"):
            PrimaryKeys.append(row['DEFINITION'])
            PrimaryKeysTypes.append(row['TYPE'])
        else:
            RegularCols.append(row['DEFINITION'])
            RegularColsTypes.append(row['TYPE'])
        if index + 1 == len(data):
            f.write("CONSTRAINT PK_" + tableName + " PRIMARY KEY (")
            for curPrimaryKeyIndex in range(len(PrimaryKeys)):
                f.write(PrimaryKeys[curPrimaryKeyIndex])
                if curPrimaryKeyIndex + 1 != len(PrimaryKeys):
                    f.write(", ")
            f.write(")\n")  
    
    f.write(");\n")
    
    f.write("DROP PROCEDURE IF EXISTS p_Update" + tableName + ";\n")
    f.write("CREATE PROCEDURE p_Update" + tableName + " ")
    for curPrimaryKeyIndex in range(len(PrimaryKeys)):
        f.write("@" + PrimaryKeys[curPrimaryKeyIndex] + " " + PrimaryKeysTypes[curPrimaryKeyIndex] + ", ")
    for curRegularCol in range(len(RegularCols)):
        f.write("@" + pStr(RegularCols[curRegularCol]) + " " + pStr(RegularColsTypes[curRegularCol]))
        if curRegularCol + 1 != len(RegularCols):
            f.write(", ")

    f.write("\nAs\n")
    f.write("Update " + tableName + "\nSET ")
    for curRegularCol in range(len(RegularCols)):
        f.write(RegularCols[curRegularCol] + " = @" + RegularCols[curRegularCol])
        if curRegularCol + 1 != len(RegularCols):
            f.write(", ")
    f.write("\nWHERE")
    for curPrimaryKeyIndex in range(len(PrimaryKeys)):
        f.write(" " + PrimaryKeys[curPrimaryKeyIndex] + " = @" + PrimaryKeys[curPrimaryKeyIndex])
    f.write(";")
    
    f.close()
    print(data)

def TakeApartCSV(filePath):
    tableName = filePath.split('/')[-1][:-4]
    curFilePath = filePath.replace(filePath.split('/')[-1], "")
    df = pd.read_csv(filePath)
    CreateTable(curFilePath, tableName, df[['DEFINITION', 'TYPE', 'DESCRIPTION1', 'DESCRIPTION2', 'DESCRIPTION3']])

def TakeApartInnerFile():
    with open("./tmp.txt") as f:
        for line in f:
            line = line.rstrip()
            if not line or line[0] == '\n':
                continue
            print(line)

def WgetAndTakeApart(sqlFilesLocation):
    fileBeginning = ""
    with open("./TableUrls.txt") as f:
        index = -1
        for line in f:
            line = line.rstrip() 
            if not line or line[0] == '\n' or line[0] == '#':
                continue 
            index += 1
            if index == 0:
                fileBeginning = line  
                continue
            fileLocation = sqlFilesLocation + "/" + line.split()[0] + ".csv"
            if os.path.exists(fileLocation):
                os.remove(fileLocation)
            wget.download(fileBeginning + line.split()[1], fileLocation)

def CheckFiles(sqlFilesLocation):
    filesCurrentlyExist = False
    for file in os.listdir(sqlFilesLocation):
        filesCurrentlyExist = True
        if file.endswith(".csv"):
            TakeApartCSV(os.path.join(sqlFilesLocation, file))
        if file.endswith(".sql"):
            ProcessSqlFile(os.path.join(sqlFilesLocation, file))
    if filesCurrentlyExist == False:
        return 500
    else:
        return 50

def main():
    f = open(os.devnull, 'w')
    sys.stdout = f
    timeToWaitMillis = 50
    sqlFilesLocation = "/home/ec2-user/ToDoMySql"
    filesCurrentlyExist = False
    WgetAndTakeApart(sqlFilesLocation)
    while True:
        sleep(timeToWaitMillis * 0.001) 
        timeToWaitMillis = CheckFiles(sqlFilesLocation)
        
if __name__ == "__main__":
    main()
