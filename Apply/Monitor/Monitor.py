import time
import threading
import socket
import sys
import json
import copy
import operator
try:
    import Common.MyEnum as MyEnum
    import Common.MyParser as MyParser
    import Common.GetDataFromServer as GetData
    import Apply.Monitor.ParseMon as ParseMon
except ImportError:
    import MyEnum
    import MyParser
    import GetDataFromServer as GetData
    import ParseMon

DEBUG = False
DATA_MODE = MyEnum.MonNode.DATA_GEN_AUTO.value

IP_SERVER  = 'localhost'
PORT_NODE = 9407
DELTA_TIME = 2.0
SAMPLE_ON_CIRCLE = 10
numberNode = 10
ORDER_NODE = 0
ORDER_VALUE = 1

eventStartMon = threading.Event()
bStop = False
session = -1

lockData = threading.Lock()
currentData = []
topK = []
delta = []

#socket to connect to server
sock = socket.socket()

################################################################################
#read from file config to get information
def readConfig():
    global myName, numberNode, addName, fileData, DEBUG, DATA_MODE, IP_SERVER, PORT_NODE, DELTA_TIME, SAMPLE_ON_CIRCLE
    addName = ''
    fName = 'monConfig'
    try:
        addName = sys.argv[1]
    except Exception:
        pass
    myName = addName

    if (DATA_MODE == MyEnum.MonNode.DATA_GEN_AUTO.value):
        fileData = open('data' + str(addName) + '.dat', 'r')

    fName += str(addName) + '.cfg'

    arg = ParseMon.readConfig(fName)
    if (arg == None):
        return
    myName = arg.NAME + str(time.time())
    DEBUG = arg.DEBUG
    DATA_MODE = arg.DATA_MODE
    IP_SERVER = arg.IP_SERVER
    PORT_NODE = arg.PORT_NODE
    DELTA_TIME = arg.DELTA_TIME
    SAMPLE_ON_CIRCLE = arg.SAMPLE_ON_CIRCLE
    numberNode = arg.NUMBER_NODE

def init():
    readConfig()
    for i in range(numberNode):
        delta.append(0)

#create message to send to server
def createMessage(strRoot = '', arg = {}):
    strResult = str(strRoot)
    for k, v in arg.items():
        strResult = strResult + ' ' + str(k) + ' ' + str(v)

    return strResult

#server send data to update argument
def updateArg(arg):
    global eventStartMon, topK
    data = arg.data[0]
    data = json.loads(data)
    if (arg.top != None):
        top = arg.top[0]
        topK = json.loads(top)
    for i in range(len(data)):
        index = data[i][ORDER_NODE]
        val = data[i][ORDER_VALUE]
        delta[index] += val

    eventStartMon.set()

#send data of nodes that server need
def sendForceData(arg):
    nodeNeed = arg.data[0]
    nodeNeed = json.loads(nodeNeed)
    arrNodeNeed = []

    minTop = currentData[topK[0]][ORDER_VALUE]
    maxF = 0
    for i in nodeNeed:
        arrNodeNeed.append([i, currentData[i][ORDER_VALUE] + delta[i]])
        if i in topK:
            minTop = min(minTop, currentData[i][ORDER_VALUE])
        else:
            maxF = max(maxF, currentData[i][ORDER_VALUE])

    arrNodeNeed = json.dumps(arrNodeNeed).replace(' ', '')
    border = max(maxF, minTop)

    dataSend = ''
    dataSend = createMessage(dataSend, {'-type':MyEnum.MonNode.NODE_SET_DATA.value})
    dataSend = createMessage(dataSend, {'-data':arrNodeNeed})
    dataSend = createMessage(dataSend, {'-border':border})
    sock.sendall(bytes(dataSend.encode()))

################################################################################
#communication with server
def workWithServer():
    global V, dtCPU, dtRAM, dtMEM, bStop, sock, currentData

    readConfig()

    try:
        # send name
        dataSend = createMessage('', {'-type': MyEnum.MonNode.NODE_SET_NAME.value})
        dataSend = createMessage(dataSend, {'-name': myName})
        currentData = getData()
        data = json.dumps(currentData).replace(' ', '')
        dataSend = createMessage(dataSend, {'-data':data})
        sock.sendall(bytes(dataSend.encode('utf-8')))

        #listen command from server
        while 1:
            try:
                dataRecv = sock.recv(1024).decode()
                if (dataRecv == ''):
                    return
                arg = parser.parse_args(dataRecv.lstrip().split(' '))
                type = arg.type[0]
            except socket.error:
                return
            except Exception:
                continue
            #server update argument
            if type == MyEnum.MonNode.SERVER_SET_ARG.value:
                updateArg(arg)
            #server need data from this node
            if type == MyEnum.MonNode.SERVER_GET_DATA.value:
                sendForceData(arg)
    except socket.error:
        pass

    finally:
        bStop = True
        sock.close()

def getData():

    if (DATA_MODE == MyEnum.MonNode.DATA_FROM_INFLUXDB.value):
        pass

    elif (DATA_MODE == MyEnum.MonNode.DATA_GEN_AUTO.value):
        global fileData
        line = fileData.readline().replace('\n','')
        if (line == ''):
            fileData.close()
            fileData = open('data' + str(addName) + '.dat', 'r')
            line = fileData.readline().replace('\n', '')

        strData = line.split(' ')
        iData = []
        for i in range(numberNode):
            iData.append([i,int(strData[i])])

        return iData

def checkValidation():
    global lockData, topK, currentData
    k = len(topK)
    lockData.acquire()
    dataCopy = copy.copy(currentData)
    lockData.release()
    for i in range(numberNode):
        dataCopy[i][ORDER_VALUE] += delta[i]
    dataCopy = sorted(dataCopy, key = operator.itemgetter(ORDER_VALUE, ORDER_NODE), reverse = True)
    #check if constrains is violated
    for i in range(numberNode - 1, k-1, -1):
        if dataCopy[i][ORDER_NODE] in topK:
            break
    #not violate
    if (i == k-1):
        return

    #violated
    border = dataCopy[i][ORDER_VALUE]

    inTop = []
    inF = []
    for j in range(i, -1, -1):
        if dataCopy[j][ORDER_NODE] in topK:
            inTop.append(dataCopy[j])
        else:
            inF.append(dataCopy[j])

    dataSend = ''
    dataSend = createMessage(dataSend, {'-type':MyEnum.MonNode.SEND_VIOLATION.value})
    inTop = json.dumps(inTop).replace(' ','')
    inF = json.dumps(inF).replace(' ','')
    dataSend = createMessage(dataSend,{'-top':inTop})
    dataSend = createMessage(dataSend,{'-f':inF})
    dataSend = createMessage(dataSend,{'-border':border})
    sock.sendall(bytes(dataSend.encode()))

#monitor data
def monData():
    global sock, currentData

    eventStartMon.wait()

    while (bStop == False):
        lockData.acquire()
        currentData = getData()
        lockData.release()
        checkValidation()
        time.sleep(DELTA_TIME)

################################################################################
################################################################################
#init connection
init()
#monData(None)
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (IP_SERVER, PORT_NODE)
    sock.connect(server_address)

    #init parser
    parser = MyParser.createParser()

    #init thread
    thMon = threading.Thread(target=monData, args=())
    thWork = threading.Thread(target=workWithServer, args=())

    thMon.start()
    thWork.start()

    #wait for all thread running
    thWork.join()
    thMon.join()

except socket.error as e:
    print(e)
    pass