import json
import threading
import socket
import time
import operator
import copy
try:
    import Common.MyEnum as MyEnum
    import Common.MyParser as MyParser
    import Apply.Coordinator.ParseCor as ParseCor
except ImportError:
    import MyEnum
    import MyParser
    import ParseCor


DEBUG = True
CONSTAN_EPS = 1
NON_CONSTAN_EPS = 0

MODE_EPS = NON_CONSTAN_EPS
eps = 0     # epsilon
ext = str('')
# init arguments
k = 4      # number elements in top
session = 0 # the number of session
band = 30  # limit bandwidth

currentBand = 0
currentK = 0
netIn  = 0
netOut = 0
countAtt = 0 #the number of monitor node equal to the number of attributes

#value and name of top elements
topK = []
nameTop = []

lstSock = []
lstName = []

dataPart = {}
sumData = {}
numNode = 0
deltaDist = {}
deltaCoor = []
b = {}

#to check whether an user connnects to
bUserConnect = False

#event to notify that initialization completed
evnInitComplete = threading.Event()
evnInitComplete.clear()

#event to notify that waiting to receive data completed
evnWaitRcv = threading.Event()
evnWaitRcv.set()
numberWait = 0

IP_SERVER  = 'localhost'
PORT_NODE = 9407
PORT_USER = 7021
NUMBER_NODE = 5
DELTA_BAND = int(band / 10)
DELTA_EPS = 1
FILE_MON_NET = 'NetWorkLoad_'+ ext+'.dat'
FILE_MON_TOP = 'Top_' + ext + '.dat'

ORDER_NODE = 0
ORDER_VALUE = 1


NUM_MONITOR = 120

#interval to update network load
TIME_CAL_NETWORK = 3.0

################################################################################
def addNetworkIn(value:int):
    global netIn
    global lockNetIn
    lockNetIn.acquire()
    netIn += value
    lockNetIn.release()

def addNetworkOut(value:int):
    global netOut
    global lockNetOut
    lockNetOut.acquire()
    netOut += value
    lockNetOut.release()

def saveNetworkLoad(currentBand, eps):
    global lockTop
    tmp = eps
    if (tmp <= DELTA_EPS):
        tmp = 0

    with open(FILE_MON_TOP, 'a') as f1:
        lockTop.acquire()
        i = 0
        for i in range(k - DELTA_K - 1, -1, -1):
            if (topK[i] != 0):
                break
        if (i >= 0 and topK[i] != 0):
            f1.write(str(topK[i]) + ' ')
            for j in range(0, i + 1):
                f1.write(nameTop[j] + ' ')
            f1.write('\n')
        lockTop.release()

    with open(FILE_MON_NET, 'a') as f:
        f.write(str(currentBand) + ' ' + str(tmp) + '\n')

def monNetwork():
    return
    global lockNetIn
    global lockNetOut
    global netIn
    global netOut
    global eps
    global countAtt, DEBUG, MODE_EPS

    while 1:
        time.sleep(TIME_CAL_NETWORK)
        lockNetIn.acquire()
        nIn = netIn / TIME_CAL_NETWORK
        netIn = 0
        lockNetIn.release()

        lockNetOut.acquire()
        nOut = netOut / TIME_CAL_NETWORK
        netOut = 0
        lockNetOut.release()

        if DEBUG:
            print('netIn = %.2f _________ netOut = %.2f_____eps = %d' %(nIn, nOut, eps) )

################################################################################
def createMessage(strRoot = '', arg = {}):
    strResult = str(strRoot)
    for k, v in arg.items():
        strResult = strResult + ' ' + str(k) + ' ' + str(v)

    return strResult

def readConfig(fName : str):
    global DEBUG, MODE_EPS, k, IP_SERVER, PORT_NODE, FILE_MON_TOP
    global PORT_USER, NUMBER_NODE, FILE_MON_NET, NUM_MONITOR, TIME_CAL_NETWORK

    arg = ParseCor.readConfig(fName)
    if (arg == None):
        return

    DEBUG = arg.DEBUG
    MODE_EPS = arg.MODE_EPS
    ext = arg.ext
    k = arg.k
    IP_SERVER = arg.IP_SERVER
    PORT_NODE = arg.PORT_NODE
    PORT_USER = arg.PORT_USER
    NUMBER_NODE = arg.NUMBER_NODE
    FILE_MON_NET = 'NetWorkLoad_'+ ext+'.dat'
    FILE_MON_TOP = 'Top_' + ext + '.dat'
    NUM_MONITOR = arg.NUM_MONITOR
    TIME_CAL_NETWORK = arg.TIME_CAL_NETWORK

def init():
    global serverForNode, serverForUser
    global lockCount, lockLst, lockTop, lockNetIn, lockNetOut
    global parser, k

    try:
        readConfig('corConfig.cfg')
    except Exception:
        pass

    #init server to listen monitor node
    serverForNode = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverForNode.bind((IP_SERVER, PORT_NODE))
    serverForNode.listen(NUMBER_NODE)

    #init server to listen user node
    serverForUser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverForUser.bind((IP_SERVER, PORT_USER))
    serverForUser.listen(1)

    #init synchronize variable
    lockCount = threading.Lock()
    lockLst = threading.Lock()
    lockTop = threading.Lock()
    lockNetIn = threading.Lock()
    lockNetOut = threading.Lock()

    #init argument parser
    parser = MyParser.createParser()

    #delete old file
    f = open(FILE_MON_NET, 'w')
    f.close()
    f = open(FILE_MON_TOP, 'w')
    f.close()

def printTop():
    global userSock, eps, lockTop, DEBUG, DELTA_K, k
    epsTmp = eps
    if (epsTmp <= DELTA_EPS):
        epsTmp = 0
    rTop = []
    rName = []

    lockTop.acquire()

    for i in range(k - DELTA_K):
        if (nameTop[i] == ''):
            break
        rTop.append(topK[i])
        rName.append(nameTop[i])

    lockTop.release()

    data = json.dumps([rTop, rName, epsTmp])
    if (DEBUG):
        print(data)

    try:
        userSock.sendall(data.encode())
    except Exception:
        return

def senDeltaToAll(top = [], f = []):

    topTmp = json.dumps(topK).replace(' ', '')
    for i in range(countAtt):
        data = ''
        arr = []
        name = lstName[i]

        data = createMessage(data, {'-type': MyEnum.MonNode.SERVER_SET_ARG.value})
        for j in top:
            arr.append([j, deltaDist[name][j]])
        for j in f:
            arr.append([j, deltaDist[name][j]])
        arr = json.dumps(arr).replace(' ','')
        data = createMessage(data, {'-data':arr})
        data = createMessage(data, {'-top':topTmp})
        s = lstSock[i]
        s.sendall(bytes(data.encode('utf-8')))

def addToDataPart(arg, nameNode = ''):
    global b, dataPart, numberWait, evnWaitRcv

    b[nameNode] = int(arg.border[0])
    dataNode = arg.data[0]
    dataNode = json.loads(dataNode)
    dataPart[nameNode] = sorted(dataNode, operator.itemgetter(0))

    numberWait -= 1
    if (numberWait == 0):
        evnWaitRcv.set()

def beginProcess():
    global evnInitComplete, numNode, sumData, countAtt, deltaDist, deltaCoor, topK, dataPart

    #init delta array
    for i in range(numNode):
        deltaCoor.append(0)
    for name in lstName:
        deltaDist[name] = copy.copy(deltaCoor)

    #init sum of all attributes
    sumPart = []
    for i in range(numNode):
        sumPart.append([i,0])

    #calculate sum of all attributes
    for name in lstName:
        for i in range(numNode):
            tmp = dataPart[name][i][ORDER_VALUE]
            sumPart[i][ORDER_VALUE] = sumPart[i][ORDER_VALUE]+ tmp

    #sort value of attributes decrease
    sumData = sorted(sumPart, key = operator.itemgetter(ORDER_VALUE, ORDER_NODE), reverse = True)

    #get top-k list
    for i in range(k):
        topK.append(sumData[i][ORDER_NODE])

    f = []
    b = {}
    border = 0
    #calculate bound
    for name in lstName:
        b[name] = 0
        dataTmp = sorted(dataPart[name], key =  operator.itemgetter(ORDER_VALUE, ORDER_NODE), reverse = True)
        for j in range(numNode - 1, k - 1, -1):
            if dataTmp[j][ORDER_NODE] in topK:
                break
        if (j < numNode - 1):
            border += dataTmp[j+1][ORDER_VALUE]
            b[name] = dataTmp[j+1][ORDER_VALUE]

        for l in range(j):
            if (not dataTmp[l][ORDER_NODE] in topK) and (not dataTmp[l][ORDER_NODE] in f):
                f.append(dataTmp[l][ORDER_NODE])

    #calculate delta for each attributes need to be distributed
    for name in lstName:
        for j in topK:
            deltaDist[name][j] = b[name] - dataPart[name][j][ORDER_VALUE] + (int)((sumPart[j][ORDER_VALUE] - border) / (countAtt + 1))

        for j in f:
            deltaDist[name][j] = b[name] - dataPart[name][j][ORDER_VALUE] + (int)((sumPart[j][ORDER_VALUE] - border) / (countAtt))

    #calculate delta for coordinator node
    for j in topK:
        tmp = sumPart[j][ORDER_VALUE] - border
        deltaCoor[j] = tmp - int(tmp / (countAtt + 1))*countAtt + eps

    for j in f:
        tmp = sumPart[j][ORDER_VALUE] - border
        deltaCoor[j] = tmp - int(tmp / (countAtt + 1)) * countAtt

    senDeltaToAll(topK, f)

    evnInitComplete.set()

def reallocate(numberNodeReallocate, nameNode = ''):
    global deltaDist, deltaCoor, dataPart
    # init delta array
    deltaTmp = []
    border = 0
    for i in range(numberNodeReallocate):
        deltaTmp.append(0)
    for name in lstName:
        deltaDist[name] = copy.copy(deltaCoor)
        border += b[name]

    # init sum of all attributes
    sumPart = []
    for i in range(numberNodeReallocate):
        indexNode = dataPart[nameNode][i][ORDER_NODE]
        sumPart.append([indexNode , deltaCoor[indexNode]])
        for name in lstName:
            sumPart[i][ORDER_VALUE] += dataPart[name][i][ORDER_VALUE]

    #calculate delta for distributes
    for name in lstName:
        for i in range(numberNodeReallocate):
            indexNode = dataPart[nameNode][i][ORDER_NODE]
            deltaDist[name][indexNode] = b[name] - dataPart[name][i][ORDER_VALUE] + (int)((sumPart[i][ORDER_VALUE] - border) / (countAtt +1))

    #calculate delta for coor
    for i in range(numberNodeReallocate):
        indexNode = dataPart[nameNode][i][ORDER_NODE]
        tmp = sumPart[i][ORDER_VALUE] - border
        deltaCoor[indexNode] = tmp - int(tmp / (countAtt + 1))  * countAtt + eps

    # sort to get topK
    sumPart = sorted(sumPart, operator.itemgetter(ORDER_VALUE, ORDER_NODE), reverse=True)
    topK = []
    for i in range(k):
        topK.append(sumPart[i][ORDER_NODE])

def doPhase3(sockIgnore = socket.socket(), top = [], f= [], nameNode = '', border = 0):
    global numberWait, numNode, b, dataPart
    #send to get force data
    datasend = ''
    nodeNeed = []
    dataPart[nameNode] = sorted(top + f, operator.itemgetter(0))
    b[nameNode] = border

    for i in range(len(top)):
        nodeNeed.append(top[i][ORDER_NODE])

    for i in range(len(f)):
        nodeNeed.append(top[i][ORDER_NODE])

    nodeNeed = json.dumps(nodeNeed).replace(' ','')
    datasend = createMessage(datasend, {'-type':MyEnum.MonNode.SERVER_GET_DATA.value})
    datasend = createMessage(datasend, {'-data':nodeNeed})

    numberWait = NUMBER_NODE - 1
    evnWaitRcv.clear()
    for sock in lstSock:
        if (sock != sockIgnore):
            sock.sendall(bytes(datasend.encode()))

    #wait for receiving data
    evnWaitRcv.wait()

    #reallocate
    reallocate(len(top) + len(f), nameNode)

    #send to all
    nodeChange = []
    for i in top:
        nodeChange.append(i[ORDER_NODE])
    for i in f:
        nodeChange.append(i[ORDER_NODE])
    senDeltaToAll(nodeChange, [])


def checkValidation(sockNode, arg, nameNode):
    top = arg.top[0]
    top = json.loads(top)

    f = arg.f[0]
    f = json.loads(f)

    border = int(arg.border[0])

    for i in range(len(f)):
        for j in range(len(top)):
            indexF = f[i][ORDER_NODE]
            indexTop = top[j][ORDER_NODE]
            valueF = f[i][ORDER_VALUE] + deltaCoor[indexF]
            valueTop = top[j][ORDER_VALUE] + deltaCoor[indexTop]
            if ( valueF > valueTop ):
                doPhase3(sockNode, top, f, nameNode, border)
                return

    maxValue = 0
    maxF = f[0][ORDER_VALUE]
    #do phase 2 next
    for i in range(len(f)):
        indexF = f[i][ORDER_NODE]
        valueF = f[i][ORDER_VALUE] + deltaCoor[indexF]
        if (valueF > maxValue):
            maxValue = valueF
        if (f[i][ORDER_VALUE] > maxF):
            maxF = f[i][ORDER_VALUE]

    deltaFix = []
    deltaNew = maxValue - border
    for i in range(len(f)):
        indexF = f[i][ORDER_NODE]
        delTmp =  deltaCoor[indexF] -deltaNew
        deltaCoor[indexF] = deltaNew
        deltaFix.append([indexF, delTmp])

    for i in range(len(top)):
        if (top[i][ORDER_VALUE] < maxF):
            indexTop = top[i][ORDER_NODE]
            delTmp =  deltaCoor[indexTop] - deltaNew
            deltaCoor[indexTop] = deltaNew
            deltaFix.append([indexTop, delTmp])

    dataSend = ''
    deltaFix = json.dumps(deltaFix).replace(' ', '')
    dataSend = createMessage(dataSend, {'-type':MyEnum.MonNode.SERVER_SET_ARG.value})
    dataSend = createMessage(dataSend, {'-data':deltaFix})
    sockNode.sendall(bytes(dataSend.encode()))


################################################################################
def workWithNode(s : socket.socket, address):
    global countAtt
    global lockCount
    global lockLst, eps, numNode

    try:
        #receive name and current partial at the node
        dataRecv = s.recv(2048).decode()
        addNetworkIn(len(dataRecv))
        try:
            if (dataRecv != ''):
                arg = parser.parse_args(dataRecv.lstrip().split(' '))
                nameNode = arg.name[0]
                dataNode = arg.data[0]
        except socket.error:
            return
        except Exception:
            pass

        lockLst.acquire()
        lstSock.append(s)
        lstName.append(nameNode)
        lockLst.release()
        dataNode = json.loads(dataNode)
        dataPart[nameNode] = dataNode

        if (countAtt == NUMBER_NODE):
            numNode = len(dataPart[nameNode])
            beginProcess()
        # else:
        evnInitComplete.wait()


        #receive current value
        while 1:
            try:
                dataRecv = s.recv(1024).decode()
                addNetworkIn(len(dataRecv))
                if (dataRecv != ''):
                    arg = parser.parse_args(dataRecv.lstrip().split(' '))
                else:
                    return

                type = arg.type[0]

                if type == MyEnum.MonNode.SEND_VIOLATION.value:
                    if (not evnWaitRcv.isSet()):
                        continue
                    checkValidation(s, arg, nameNode)
                elif type == MyEnum.MonNode.NODE_SET_DATA.value:
                    addToDataPart(arg, nameNode)

            except socket.error:
                return
            except Exception:
                continue

    except socket.error:
        pass

    finally:
        s.close()
        lockLst.acquire()
        lstSock.remove(s)
        lstName.remove(nameNode)
        lockLst.release()

        lockCount.acquire()
        countAtt -= 1
        lockCount.release()

def acceptNode(server):
    global countAtt
    global lockCount
    countAtt = 0
    while (countAtt < NUMBER_NODE):
        print('%d\n' % (countAtt))
        (nodeSock, addNode) = server.accept()
        countAtt += 1

        threading.Thread(target=workWithNode, args=(nodeSock, addNode,)).start()
################################################################################
def acceptUser(server : socket.socket):
    global  userSock
    while (1):
        (userSock, addressUser) = server.accept()
        workWithUser(userSock)

def workWithUser(s : socket.socket):
    global parser
    global bUserConnect
    bUserConnect = True
    printTop()
    try:
        while 1:
            dataRecv = s.recv(1024).decode()
            if (dataRecv == ''):
                return
            arg = parser.parse_args(dataRecv.lstrip().split(' '))
            type = arg.type[0]
    except socket.error:
        return
    finally:
        bUserConnect = False
        s.close()

################################################################################
def test():
    global numNode, countAtt, k
    lstName.append('0')
    lstName.append('1')
    numNode = 4
    countAtt = 2
    dataPart['0'] = [[0,5],[1,4],[2,6],[3,5]]
    dataPart['1'] = [[0,4],[1,6],[2,7],[3,3]]
    k = 2
    beginProcess()
################################################################################

init()

# create thread for each server
thNode = threading.Thread(target=acceptNode, args=(serverForNode,))
thNode.start()

thMon = threading.Thread(target=monNetwork, args=())
thMon.start()

thUser = threading.Thread(target=acceptUser, args=(serverForUser,))
thUser.start()

try:
    #wait for all thread terminate
    thNode.join()
    thMon.join()

    thUser.join()
except KeyboardInterrupt:
    serverForNode.close()
    serverForUser.close()