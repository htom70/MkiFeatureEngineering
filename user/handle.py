import mariadb
import numpy as np
import statistics


def getAllRecordsFromDatabase(databaseName):
    # connection = mysql.connector.connect(
    #     host="localhost",
    #     user="root",
    #     password="TOmi_1970",
    #     database="retired_transaction")

    connection = mariadb.connect(
        # pool_name="read_pull",
        # pool_size=1,
        host="store.usr.user.hu",
        user="mki",
        password="pwd",
        database=databaseName
    )
    print(connection)
    sql_select_Query = "select * from transaction order by timestamp"
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)
    result = cursor.fetchall()
    # connection.close()
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
    connection.close()
    print(f"{databaseName} created")


def getAmountPropertiesByTerm(databaseName, term, cardNumber, timestamp):
    connection = mariadb.connect(
        # pool_name="read_pull",
        # pool_size=1,
        host="store.usr.user.hu",
        user="mki",
        password="pwd",
        database=databaseName
    )
    cursor = connection.cursor()
    amountPropertiesByTerms = dict()
    if term == 'full':
        sqlQuery = f"select AVG(amount), STDDEV(amount), median(amount)  over (PARTITION BY card_number) from {databaseName}.transaction where card_number = {cardNumber};"
        keyName = term
    else:
        lowerBorder = timestamp - term
        sqlQuery = f"select AVG(amount), STDDEV(amount), median(amount)  over (PARTITION BY card_number) from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {timestamp} and {lowerBorder}"
        keyName = str(term)
    cursor.execute(sqlQuery)
    result = cursor.fetchall()
    for line in result:
        average = line[0]
        deviation = line[1]
        median = line[2]
        items = [average, deviation, median]
        amountPropertiesByTerms[keyName] = items
    cursor.close()
    connection.close()
    return amountPropertiesByTerms


def getTransacTionAmountProperties(databaseName, term, cardNumber, timestamp, amount):
    amountAggregates = dict()
    amountProperties = getAmountPropertiesByTerm(databaseName, term, cardNumber, timestamp)
    averageAmount = amountProperties.get(term)[0]
    deviationAmount = amountProperties.get(term)[1]
    medianAmount = amountProperties.get(term)[2]
    amountToAverageAmountRatio = amount / averageAmount
    amountToAverageAmountDiff = amount - averageAmount
    amountToMedianAmountRatio = amount / medianAmount
    amountToMedianAmountDiff = amount - medianAmount
    amountToDeviationAmountRatio = amount / deviationAmount
    amountToDeviationAmountDiff = amount - deviationAmount

    amountAggregates['amountToAverageAmountRatio'] = amountToAverageAmountRatio
    amountAggregates['amountToAverageAmountDiff'] = amountToAverageAmountDiff
    amountAggregates['amountToMedianAmountRatio'] = amountToMedianAmountRatio
    amountAggregates['amountToMedianAmountDiff'] = amountToMedianAmountDiff
    amountAggregates['amountToDeviationAmountRatio'] = amountToDeviationAmountRatio
    amountAggregates['amountToDeviationAmountDiff'] = amountToDeviationAmountDiff
    return amountAggregates


def getTransacTionNumberPropertiesByTerm(databaseName, term, cardNumber, timestamp):
    connection = mariadb.connect(
        # pool_name="read_pull",
        # pool_size=1,
        host="store.usr.user.hu",
        user="mki",
        password="pwd",
        database=databaseName
    )
    cursor = connection.cursor()
    transactionNumberPropertiesByTerms = dict()
    lowerDailyBorder = term - 1
    sqlQuery = f"select COUNT(*) from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {timestamp} and {lowerDailyBorder}"
    cursor.execute(sqlQuery)
    transactionNumberOnCurrenDay = cursor.fetchall()
    if term == 'full':
        sqlQuery = f"select AVG(amount), median(amount)  over () from {databaseName}.transaction where card_number = {cardNumber};"
        keyName = term
    else:
        lowerBorder = timestamp - term
        sqlQuery = f"select AVG(amount), median(amount)  over () from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {timestamp} and {lowerBorder}"
        keyName = str(term)
    cursor.execute(sqlQuery)
    result = cursor.fetchall()
    # averageTransactionNumber = 0
    # medianTransactionNumber = 0
    if term == 'full':
        averageTransactionNumber = result[0] / 365
        medianTransactionNumber = result[1] / 365
    else:
        averageTransactionNumber = result[0] / term
        medianTransactionNumber = result[1] / term
    transactionNumberToAverageDailyTransactionNumberRatio = transactionNumberOnCurrenDay / averageTransactionNumber
    transactionNumberToAverageDailyTransactionNumberDiff = transactionNumberOnCurrenDay - averageTransactionNumber
    transactionNumberToMedianDailyTransactionNumberRatio = transactionNumberOnCurrenDay / medianTransactionNumber
    transactionNumberToMedianDailyTransactionNumberDiff = transactionNumberOnCurrenDay - medianTransactionNumber
    transactionNumberPropertiesByTerms[
        'transactionNumberToAverageDailyTransactionNumberRatio'] = transactionNumberToAverageDailyTransactionNumberRatio
    transactionNumberPropertiesByTerms[
        'transactionNumberToAverageDailyTransactionNumberDiff'] = transactionNumberToAverageDailyTransactionNumberDiff
    transactionNumberPropertiesByTerms[
        'transactionNumberToMedianDailyTransactionNumberRatio'] = transactionNumberToMedianDailyTransactionNumberRatio
    transactionNumberPropertiesByTerms[
        'transactionNumberToMedianDailyTransactionNumberDiff'] = transactionNumberToMedianDailyTransactionNumberDiff
    return transactionNumberPropertiesByTerms


def getTransacTionNumberProperties(databaseName, term, currentCardNumber, currentTimestamp, currentAmount):
    pass


def getExtendedTransactionFeatures(transactionFeature, transactionAmountProperties, transactionNumberProperties,
                                   transacTionIntervalProperties):
    pass


def getAverageInterval(input, term):
    sum=0
    length= len(input)
    for i in range(1,length,1):
        diff=input[i]-input[i-1]
        sum+=diff
    if term=='full':
        average=sum/365
    else:
        average=sum/term
    return average



def getTransacTionIntervalProperties(databaseName, term, cardNumber, timestamp):
    connection = mariadb.connect(
        # pool_name="read_pull",
        # pool_size=1,
        host="store.usr.user.hu",
        user="mki",
        password="pwd",
        database=databaseName
    )
    cursor = connection.cursor()
    intervalPropertiesByTerm=dict()
    sqlQuery = f"select timestamp from {databaseName}.transaction where card_number = {cardNumber} and timestamp <= {timestamp}  order by timestamp desc limit 2;"
    cursor.execute(sqlQuery)
    result = cursor.fetchall()
    currentInterval = result[0] - result[1]
    if term == 'full':
        sqlQuery = f"select timestamp from {databaseName}.transaction where card_number = {cardNumber} and timestamp <= {timestamp}  order by timestamp desc;"
    else:
        lowerBorder = timestamp - term
        sqlQuery = f"select timestamp from {databaseName}.transaction where card_number = {cardNumber} and timestamp between {timestamp} and {lowerBorder}"
    cursor.execute(sqlQuery)
    result = cursor.fetchall()
    averageInterval = statistics.mean(result)
    medianInterval = statistics.median(result)
    intervalToAverageIntervalRatio = currentInterval / averageInterval
    intervalToAverageIntervalDiff = currentInterval - averageInterval
    intervalToMedianIntervalRatio = currentInterval / medianInterval
    intervalToMedianIntervalDiff = currentInterval - medianInterval
    intervalPropertiesByTerm['intervalToAverageIntervalRatio']=intervalToAverageIntervalRatio
    intervalPropertiesByTerm['intervalToAverageIntervalDiff']=intervalToAverageIntervalDiff
    intervalPropertiesByTerm['intervalToMedianIntervalRatio']=intervalToMedianIntervalRatio
    intervalPropertiesByTerm['intervalToMedianIntervalDiff']=intervalToMedianIntervalDiff
    return intervalPropertiesByTerm


def saveExtendedDataset(extendedDataset):
    pass


if __name__ == '__main__':
    databaseNames = ["card_10000_5_i"]
    # databaseNames = ["card_10000_5_i", "card_100000_1_i", "card_250000_02_i", "card_i"]
    terms = [3, 7, 15, 30, 'full']
    for databaseName in databaseNames:
        aggregatedAndImputedDatebaseName = databaseName + "_a"
        createDatabase(aggregatedAndImputedDatebaseName)
        dataset = getAllRecordsFromDatabase(databaseName)
        extendedDataset = list()
        transactionFeatures = dataset[:, 1:-1]
        for transactionFeature in transactionFeatures:
            currentCardNumber = transactionFeature[:, :1]
            currentTimestamp = transactionFeature[:, 2:3]
            currentAmount = transactionFeature[:, 3:4]
            for term in terms:
                transactionAmountProperties = getTransacTionAmountProperties(databaseName, term, currentCardNumber,
                                                                             currentTimestamp, currentAmount)
                transactionNumberProperties = getTransacTionNumberProperties(databaseName, term, currentCardNumber,
                                                                             currentTimestamp, currentAmount)
                transacTionIntervalProperties = getTransacTionIntervalProperties(databaseName, term, currentCardNumber,
                                                                                 currentTimestamp, currentAmount)
                extendedTransactionRecord = getExtendedTransactionFeatures(transactionFeature,
                                                                           transactionAmountProperties,
                                                                           transactionNumberProperties,
                                                                           transacTionIntervalProperties)
                extendedDataset.append(extendedTransactionRecord)
        saveExtendedDataset(databaseName, extendedDataset)
