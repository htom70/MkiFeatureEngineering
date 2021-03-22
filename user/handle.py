import mariadb
import numpy as np
import statistics
import math


def getConnection(databaseName):
    connectionContainer = dict()
    if connectionContainer.get(databaseName) is None:
        connection = mariadb.connect(
            pool_name=databaseName,
            pool_size=8,
            host="store.usr.user.hu",
            user="mki",
            password="pwd",
            database=databaseName
        )
        connectionContainer[databaseName] = connection
    return connectionContainer.get(databaseName)


def getAllRecordsFromDatabase(databaseName):
    connection = getConnection(databaseName)
    print(connection)
    sql_select_Query = "select * from transaction order by timestamp"
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    numpy_array = np.array(result)
    length = len(numpy_array)
    print(f'{databaseName} beolvasva, rekordok száma: {length}')
    return numpy_array[:, :]


def createDatabase(databaseName):
    connection = mariadb.connect(
        pool_name="create_pool",
        pool_size=1,
        host="store.usr.user.hu",
        user="mki",
        password="pwd")

    # connection = mysql.connector.connect(
    #     pool_name="create_pool",
    #     pool_size=1,
    #     host="localhost",
    #     user="root",
    #     password="TOmi_1970")

    cursor = connection.cursor()
    sqlDropDatabaseScript = "DROP DATABASE IF EXISTS " + databaseName
    cursor.execute(sqlDropDatabaseScript)
    connection.commit()
    sqlCreateSchemaScript = "CREATE DATABASE IF NOT EXISTS " + databaseName + " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    cursor.execute(sqlCreateSchemaScript)
    connection.commit()
    cursor.execute("USE " + databaseName)
    file = open("SQL create table transaction.txt", "r")
    sqlCreataTableScript = file.read()
    cursor.execute("DROP TABLE IF EXISTS transaction")
    cursor.execute(sqlCreataTableScript)
    connection.commit()
    cursor.close()
    connection.close()
    print(f"{databaseName} created")


def getFirstDateFromJulian(databaseName):
    connection = getConnection(databaseName)
    cursor = connection.cursor()
    sqlQuery = f"select timestamp from {databaseName}.transaction order by timestamp asc limit 1;"
    cursor.execute(sqlQuery)
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    firstDate = math.floor(result[0])
    return firstDate


def getTransacTionAmountProperties(databaseName, term, cardNumber, timestamp, amount, firstDate):
    connection = getConnection(databaseName)
    cursor = connection.cursor()
    if term == 'earlier':
        sqlQuery = f"select AVG(amount), STDDEV(amount), median(amount)  over (PARTITION BY amount) from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {firstDate} and {timestamp};"
    else:
        lowerBorder = timestamp - term
        sqlQuery = f"select AVG(amount), STDDEV(amount), median(amount)  over (PARTITION BY amount) from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {lowerBorder} and {timestamp}"
    cursor.execute(sqlQuery)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    temp = result[0][0]
    averageAmount = temp if temp is not None else 0
    temp = result[0][1]
    deviationAmount = temp if temp is not None else 0
    temp = result[0][2]
    medianAmount = temp if temp is not None else 0
    amountToAverageAmountRatio = amount / averageAmount if averageAmount != 0 else 0
    amountToAverageAmountDiff = amount - averageAmount
    amountToMedianAmountRatio = amount / medianAmount if medianAmount != 0 else 0
    amountToMedianAmountDiff = amount - medianAmount
    amountToDeviationAmountRatio = amount / deviationAmount if deviationAmount != 0 else 0
    amountToDeviationAmountDiff = amount - deviationAmount
    resultList = [amountToAverageAmountRatio, amountToAverageAmountDiff, amountToMedianAmountRatio,
                  amountToMedianAmountDiff, amountToDeviationAmountRatio, amountToDeviationAmountDiff]
    return resultList


def getTransacTionNumberProperties(databaseName, term, cardNumber, timestamp, firstDate):
    connection = getConnection(databaseName)
    cursor = connection.cursor()
    lowerDailyBorder = term - 1
    sqlQuery = f"select COUNT(*) from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {lowerDailyBorder} and {timestamp}"
    cursor.execute(sqlQuery)
    transactionNumberOnCurrentDay = cursor.fetchall()
    if term == 'earlier':
        firstDay = firstDate
        sqlQuery = f"select COUNT(*) from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {firstDate} and {timestamp}"
    else:
        firstDay = math.floor(timestamp - term)
        lowerBorder = timestamp - term
        sqlQuery = f"select AVG(amount), median(amount)  over () from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {lowerBorder} and {timestamp}"
    dayNumber = math.floor(timestamp - firstDay)
    transactionNumberList = list()
    upperBorder = math.floor(timestamp)
    while dayNumber > 0:
        lowerBorder = upperBorder - 1
        sqlQuery = f"select COUNT(*) from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {lowerBorder} and {upperBorder}"
        cursor.execute(sqlQuery)
        transactioNumber = cursor.fetchone()
        transactionNumberList.append(transactioNumber)
        dayNumber = dayNumber - 1
        upperBorder = upperBorder - 1
    cursor.close()
    connection.close()
    averageTransactionNumber = statistics.mean(transactionNumberList)
    medianTransactionNumber = statistics.median(transactionNumberList)
    transactionNumberToAverageDailyTransactionNumberRatio = transactionNumberOnCurrentDay / averageTransactionNumber if averageTransactionNumber != 0 else 0
    transactionNumberToAverageDailyTransactionNumberDiff = transactionNumberOnCurrentDay - averageTransactionNumber
    transactionNumberToMedianDailyTransactionNumberRatio = transactionNumberOnCurrentDay / medianTransactionNumber if medianTransactionNumber != 0 else 0
    transactionNumberToMedianDailyTransactionNumberDiff = transactionNumberOnCurrentDay - medianTransactionNumber
    resultList = [transactionNumberToAverageDailyTransactionNumberRatio,
                  transactionNumberToAverageDailyTransactionNumberDiff,
                  transactionNumberToMedianDailyTransactionNumberRatio,
                  transactionNumberToMedianDailyTransactionNumberDiff]
    return resultList


def getTransacTionIntervalProperties(databaseName, term, cardNumber, timestamp, firstDate):
    connection = getConnection(databaseName)
    cursor = connection.cursor()
    sqlQuery = f"select timestamp from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {firstDate} and {timestamp}  order by timestamp desc limit 2;"
    cursor.execute(sqlQuery)
    result = cursor.fetchall()
    currentInterval = result[0] - result[1]
    if term == 'earlier':
        sqlQuery = f"select timestamp from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {firstDate} and {timestamp} and  order by timestamp desc;"
    else:
        lowerBorder = timestamp - term
        sqlQuery = f"select timestamp from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {lowerBorder} and {timestamp}"
    cursor.execute(sqlQuery)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    averageInterval = statistics.mean(result)
    medianInterval = statistics.median(result)
    intervalToAverageIntervalRatio = currentInterval / averageInterval if averageInterval != 0 else 0
    intervalToAverageIntervalDiff = currentInterval - averageInterval
    intervalToMedianIntervalRatio = currentInterval / medianInterval if medianInterval != 0 else 0
    intervalToMedianIntervalDiff = currentInterval - medianInterval
    resultList = [intervalToAverageIntervalRatio, intervalToAverageIntervalDiff, intervalToMedianIntervalRatio,
                  intervalToMedianIntervalDiff]
    return resultList


def saveExtendedDataset(databaseName, extendedDataset):
    connection = getConnection(databaseName)


if __name__ == '__main__':
    databaseNames = ["card_10000_5_i"]
    # databaseNames = ["card_10000_5_i", "card_100000_1_i", "card_250000_02_i", "card_i"]
    terms = [3, 7, 15, 30, 'earlier']
    for databaseName in databaseNames:
        aggregatedAndImputedDatebaseName = databaseName + "_a"
        createDatabase(aggregatedAndImputedDatebaseName)
        dataset = getAllRecordsFromDatabase(databaseName)
        firstDate = getFirstDateFromJulian(databaseName)
        extendedDataset = list()
        transactionFeatures = dataset[:, 1:-1]
        tranactionLabels = dataset[:, -1:]
        length = len(transactionFeatures)
        for i in range(length):
            transactionFeature = transactionFeatures[i]
            currentCardNumber = math.floor(transactionFeature[0])
            currentTimestamp = transactionFeature[2]
            currentAmount = transactionFeature[3]
            transactionFeatureList = list(transactionFeature)
            for term in terms:
                transactionAmountProperties = getTransacTionAmountProperties(databaseName, term, currentCardNumber,
                                                                             currentTimestamp, currentAmount, firstDate)
                transactionNumberProperties = getTransacTionNumberProperties(databaseName, term, currentCardNumber,
                                                                             currentTimestamp, firstDate)
                transactionIntervalProperties = getTransacTionIntervalProperties(databaseName, term, currentCardNumber,
                                                                                 currentTimestamp, firstDate)

                transactionFeatureList.extend(transactionAmountProperties)
                transactionFeatureList.extend(transactionNumberProperties)
                transactionFeatureList.extend(transactionIntervalProperties)
                label = tranactionLabels[i][0]
                record = transactionFeatureList.append(label)
                recordArray = np.array(record)
                extendedDataset.append(tuple(recordArray))
                print(len(extendedDataset))
        saveExtendedDataset(databaseName, extendedDataset)
