import os, time



def WriteFile(folderPath, fileName, dataTable, addStampDate):
    prefix = ""
    if addStampDate:
        t = time.localtime()
        prefix = time.strftime("%Y%m%d%H%M%S", t)
    filepath = os.path.join(folderPath, prefix + "_" + fileName + ".csv")
    with open(filepath, 'w') as f:
        dataTable.to_csv(f, header=False)