# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import os
import pandas  as pd


import xml.etree.ElementTree as ET
import csv
import glob
import re
TextPrefix = '{http://bmi.ch.abb.com/kbs/'
TextCI = TextPrefix + 'ci}'
TextNM = TextPrefix + 'nm}'
TextDB = TextPrefix + 'db}'
TextKY = TextPrefix + 'ky}'
TextRR = TextPrefix + 'rr}'
TextPK = TextPrefix + 'pk}'
TextPV = TextPrefix + 'pv}'
#the various attributes available in reports
templateKey = '{http://bmi.ch.abb.com/kbs/rr}tplid_template_tplname'
descKey = '{http://bmi.ch.abb.com/kbs/db}description'
keyKey = '{http://bmi.ch.abb.com/kbs/nm}key'
RepnameKey = '{http://bmi.ch.abb.com/kbs/nm}repname'
valuerepitemKey = '{http://bmi.ch.abb.com/kbs/rr}value_repitem_ritext'
ritextKey = '{http://bmi.ch.abb.com/kbs/nm}ritext'
iidKey = '{http://bmi.ch.abb.com/kbs/ci}iid'
value_signalitem_signameKey = '{http://bmi.ch.abb.com/kbs/rr}value_signalitem_signame'
signameKey = '{http://bmi.ch.abb.com/kbs/nm}signame'
parentidKey = '{http://bmi.ch.abb.com/kbs/ci}parentid'
foldernameKey = TextNM + 'foldername'
channameKey = TextRR + 'channr_signalchannel_channame'
valuesignal_signalitem_signameKey = '{http://bmi.ch.abb.com/kbs/rr}valuesignal_signalitem_signame'

valueKey = '{http://bmi.ch.abb.com/kbs/pv}value'

ns = { 'rr' : 'http://bmi.ch.abb.com/kbs/rr', 'db' : 'http://bmi.ch.abb.com/kbs/db', 'nm' : 'http://bmi.ch.abb.com/kbs/nm' }

ErrorTagsNotinLog= None
ErrorTagsInLog = None
Nosignals = None
SignalsWithoutLogs = None

LogXML = None

SignalXML = None


#all of the atrribs available in log tags
LogKeys = {
    'iid': TextCI + 'iid',
    'parentid': TextCI + 'parentid',
    'typeid': TextCI + 'typeid',
    'remoteid': TextCI + 'remoteid',
    'ricode': TextKY + 'ricode',
    'sigid': TextCI + 'sigid',
    'ritext': TextNM + 'ritext',
    'aliasname': TextDB + 'aliasname',
    'riclass': TextDB + 'riclass',
    'riunit': TextDB + 'riunit',
    'description': TextDB + 'description',
    'ricodea': TextDB + 'ricodea',
    #'ricodeb': TextDB + 'ricodeb',
    'textpropid': TextDB + 'textpropid',
    'binpropid': TextDB + 'binpropid',
    'aggrfunca': TextDB + 'aggrfunca',
    'aggrfuncb': TextDB + 'aggrfuncb',
    'aggrfuncc': TextDB + 'aggrfuncc',
    'aggrfuncd': TextDB + 'aggrfuncd',
    'aggrfunce': TextDB + 'aggrfunce',
    'aggrfuncf': TextDB + 'aggrfuncf',
    'aggrfuncg': TextDB + 'aggrfuncg',
    'aggrfunch': TextDB + 'aggrfunch',
    'aggrfunci': TextDB + 'aggrfunci',
    'aggrfuncj': TextDB + 'aggrfuncj',
    'aggrfunck': TextDB + 'aggrfunck',
    'aggrfuncl': TextDB + 'aggrfuncl',
    'aggrfuncm': TextDB + 'aggrfuncm',
    'ricodea_repitem_ritext': TextRR + 'ricodea_repitem_ritext',
    'ricodeb_repitem_ritext': TextRR + 'ricodeb_repitem_ritext',
    'ricodec_repitem_ritext': TextRR + 'ricodec_repitem_ritext',
    'ricoded_repitem_ritext': TextRR + 'ricoded_repitem_ritext',
    'ricodee_repitem_ritext': TextRR + 'ricodee_repitem_ritext',
    'ricodef_repitem_ritext': TextRR + 'ricodef_repitem_ritext',
    'ricodeg_repitem_ritext': TextRR + 'ricodeg_repitem_ritext',
    'ricodeh_repitem_ritext': TextRR + 'ricodeh-repitem_ritext',
    'ricodei_repitem_ritext': TextRR + 'ricodei_repitem_ritext',
    'ricodej_repitem_ritext': TextRR + 'ricodej_repitem_ritext',
    'ricodek_repitem_ritext': TextRR + 'ricodek_repitem_ritext',
    'ricodel_repitem_ritext': TextRR + 'ricodel_repitem_ritext',
    'ricodem_repitem_ritext': TextRR + 'ricodem_repitem_ritext',
    'contents': TextDB + 'contents',
    'riscale': TextDB + 'riscale',
    'valuesignal': TextDB + 'valuesignal',
    'valuesignal_signalitem_signame': TextRR + 'valuesignal_signalitem_signame',
    'sortricode': TextDB + 'sortricode',
    'cumricode': TextDB + 'cumricode',
    'inflowsignal': TextDB + 'inflowsignal',
    'outflowsignal': TextDB + 'outflowsignal',
    'manualsignal': TextDB + 'manualsignal'}

objunitKey = TextDB + 'objunit'


Accum_Check_Tags = []
Accum_Refs = pd.DataFrame(columns = ['Report Name', 'Tag', 'Report Type'])

RHR_Check_Tags = []
RHR_Refs = pd.DataFrame(columns = ['Report Name', 'Tag', 'Report Type'])

CHAN_9_Refs = pd.DataFrame(columns = ['Report Name', 'Tag', 'Report Type'])

CALC_CHAN_9_Refs = pd.DataFrame(columns = ['Report Name', 'Tag', 'Report Type'])
CALC_CTR_Refs = pd.DataFrame(columns = ['Report Name', 'Tag', 'Report Type'])
CTR_Refs = pd.DataFrame(columns = ['Report Name', 'Tag', 'Report Type'])
ReportList = pd.DataFrame(columns=['Report Name', 'Type','Folder'])

ProcessingFolder = "/Users/ryanplester/WRA Dropbox/Projects/Sherritt Historian Replacement/Report Processing/Sherritt CSV/"


#{'real_person': 'http://people.example.com', 'role': 'http://characters.example.com'}


#main parsing function
def xmlParse():
    # Use a breakpoint in the code line below to debug your script.
    files = glob.glob(ProcessingFolder + '*')
    for f in files:
        os.remove(f)

    #init the csv files.  Global because the various report types can all write to them
    CSVFile1  = open(ProcessingFolder + "@No KM Lookup In Logs.csv", 'w', encoding='UTF8')
    global ErrorTagsInLog
    ErrorTagsInLog = csv.writer(CSVFile1)
    ErrorTagsInLog.writerow(['Report Name','RItext', 'Description', 'Signal'])
    CSVFile2 = open(ProcessingFolder + "@No KM Lookup No Logs.csv", 'w', encoding='UTF8')
    global ErrorTagsNotinLog
    ErrorTagsNotinLog = csv.writer(CSVFile2)
    CSVFile3 = open(ProcessingFolder + "@No Signal.csv", 'w', encoding='UTF8')
    global Nosignals
    NoSignals = csv.writer(CSVFile3)
    CSVFile4 = open(ProcessingFolder + "@Signals Without Logs.csv", 'w', encoding='UTF8')
    global SignalsWithoutLogs
    SignalsWithoutLogs = csv.writer(CSVFile4)
    mypath = '/Users/ryanplester/WRA Dropbox/Projects/Sherritt Historian Replacement/KM reports trends XML (1)'

    AccumCheckTagFile = open('/Users/ryanplester/Downloads/Calc References - Accumulator.csv')
    CSVAccumCheckTagFile = csv.DictReader(AccumCheckTagFile)
    global Accum_Check_Tags
    for val in CSVAccumCheckTagFile:
        TagName = val['nm_ritext']
        Accum_Check_Tags.append(TagName)

    RHRCheckTagFile = open('/Users/ryanplester/Downloads/Calc References - RHRs.csv')
    CSVRHRCheckTagFile = csv.DictReader(RHRCheckTagFile)
    global RHR_Check_Tags
    for val in CSVRHRCheckTagFile:
        TagName = val['nm_ritext']
        RHR_Check_Tags.append(TagName)
        print(TagName)

    Manual_Tags()

    #iterate through the xml files in the path
    for path, subdirs, files in os.walk(mypath):
        for name in files:
            if ".xml" in name and "old" not in name and 'User' not in name:
                fullPath = os.path.join(path, name)
                print (fullPath)
                ProcessFile(fullPath)

    ReportList.to_csv((ProcessingFolder + "@@Report List.csv"))
    #close the error files
    CSVFile1.close()
    CSVFile2.close()
    CSVFile3.close()
    CSVFile4.close()

    #csvFile = open(ProcessingFolder + "@Accum_References.csv", 'w', encoding='UTF8')
    #myWriter = csv.writer(csvFile)
    Accum_Refs.to_csv(ProcessingFolder + "@Accum_References.csv")
    RHR_Refs.to_csv(ProcessingFolder + "@RHR_References.csv")
    CHAN_9_Refs.to_csv(ProcessingFolder + "@CHAN9_References.csv")
    CALC_CHAN_9_Refs.to_csv(ProcessingFolder + "@CALC_CHAN9_References.csv")
    CALC_CTR_Refs.to_csv(ProcessingFolder + "@CALC_CTR_References.csv")
    CTR_Refs.to_csv(ProcessingFolder + "@CTR_References.csv")


def Manual_Tags():
    InitSignalXML()
    filter = './/SIGNALITEM[@' + channameKey + '= "CHAN_9"]'
    ManualSignalElements = SignalXML.findall(filter)
    ManualTagList = pd.DataFrame(columns=['Name', 'Type', 'Min','Max', 'Description'])
    AVEVATagList = pd.DataFrame(columns=[':(AnalogTag)TagName','Description','EngUnits'])
    AVEVAUnits = pd.DataFrame(columns=[':(EngineeringUnit)Unit'])
    for SignalElement in ManualSignalElements:
        SignalTagName = XMLAttribValue(SignalElement,signameKey)
        LogElement = LogTagFromSignalTag(SignalTagName)
        if LogElement is not None:
            LogTagName = XMLAttribValue(LogElement,ritextKey)
            LogAlias = XMLAttribValue(LogElement,LogKeys['aliasname'])
            SignalUnits = XMLAttribValue(SignalElement,objunitKey)
            AVEVATagName = re.sub('\W+', '_', LogAlias)
            LogClass = XMLAttribValue(LogElement,LogKeys['riclass'])
            AVEVATagName = AVEVATagName.replace('_' + LogClass, '')

            ManualTagList = ManualTagList.append({'Name': AVEVATagName, 'Type': 'Analog','Min': 0, 'Max': 10000000, 'Description': LogTagName}, ignore_index=True)
            AVEVATagList = AVEVATagList.append({':(AnalogTag)TagName': AVEVATagName, 'Description': LogTagName, 'EngUnits': SignalUnits}, ignore_index=True)

            tempdf = pd.DataFrame(columns=[':(EngineeringUnit)Unit'])
            tempdf = AVEVAUnits[(AVEVAUnits[':(EngineeringUnit)Unit'] == SignalUnits)]
            if tempdf.shape[0] == 0:
                AVEVAUnits = AVEVAUnits.append({':(EngineeringUnit)Unit': SignalUnits}, ignore_index=True)
    AVEVAUnits.insert(1,'DefaultTagRate', 10000)
    AVEVAUnits.insert(2,'IntegralDivisor','')
    AVEVAUnits.insert(3,'BasicSymbol', '?')
    AVEVAUnits.insert(4, 'Dimension', 'Unknown')
    AVEVAUnits.insert(5, 'System', 'General')
    AVEVAUnits.to_csv(ProcessingFolder + "@@manual Tags AVEVA Units.txt",sep='\t',line_terminator='\r\n', index=False)


    ManualTagList.to_csv(ProcessingFolder + "@@manual Tags DreamReport.csv",';', index=False)

    #Aveva Tag List Columns
    AVEVATagList.insert(2,'IOServerComputerName','$local')
    AVEVATagList.insert(3, 'IOServerAppName', 'InSQL_MDAS')
    AVEVATagList.insert(4, 'TopicName', 'MDAS')
    AVEVATagList.insert(5, 'ItemName', '')
    AVEVATagList.insert(6, 'AcquisitionType', 'Manual')
    AVEVATagList.insert(7, 'StorageType', 'Delta')
    AVEVATagList.insert(8, 'AcquisitionRate', '0')
    AVEVATagList.insert(9, 'StorageRate', '0')
    AVEVATagList.insert(10, 'TimeDeadband', '0')
    AVEVATagList.insert(11, 'SamplesInAI', '0')
    AVEVATagList.insert(12, 'AIMode', 'All')
    AVEVATagList.insert(14, 'MinEU', '0')
    AVEVATagList.insert(15, 'MaxEU', '10000000')
    AVEVATagList.insert(16, 'MinRaw', '0')
    AVEVATagList.insert(17, 'MaxRaw', '10000000')
    AVEVATagList.insert(18, 'Scaling', 'None')
    AVEVATagList.insert(19, 'RawType', 'MSFloat')
    AVEVATagList.insert(20, 'IntegerSize', '0')
    AVEVATagList.insert(21, 'Sign', '')
    AVEVATagList.insert(22, 'ValueDeadband', '0')
    AVEVATagList.insert(23, 'InitialValue', '0')
    AVEVATagList.insert(24, 'CurrentEditor', '0')
    AVEVATagList.insert(25, 'RateDeadband', '0')
    AVEVATagList.insert(26, 'InterpolationType', 'System Default')
    AVEVATagList.insert(27, 'RolloverValue', '0')
    AVEVATagList.insert(28, 'ServerTimeStamp', 'No')
    AVEVATagList.insert(29, 'DeadbandType', 'TimeValue')
    AVEVATagList.insert(30, 'TagId', '')
    AVEVATagList.insert(31, 'ChannelStatus', '1')
    AVEVATagList.insert(32, 'AITag', '0')
    AVEVATagList.insert(33, 'AIHistory', 'FALSE')
    AVEVATagList.insert(34, 'SourceTag', '')
    AVEVATagList.insert(35, 'SourceServer', '')
    AVEVATagList.insert(36, 'SourceTagId', '')
    AVEVATagList.insert(37, 'ShardId', '{00000000-0000-0000-0000-000000000000}')


    AVEVATagList.to_csv(ProcessingFolder + "@@manual Tags AVEVA.txt",sep='\t',line_terminator='\r\n', index=False)

def ProcessFile(path):
    try:
        #get the root node and the KM tag lookup
        root_node = ET.parse(path).getroot()
        KMLookupCSV = open("/Users/ryanplester/Downloads/KM Lookup - Sheet11.csv")
        KMLookupReader = csv.reader(KMLookupCSV)
        data = list(KMLookupReader)
        KMLookupCSV.close()
        getFlowSheets(root_node,data)

        for tag in root_node.findall('.//REPORT30'):
            reportFolder = FindReportFolder(tag, root_node)
            folderName = XMLAttribValue(reportFolder, foldernameKey)
            folderPath = FolderPathFromReportXMLNode(tag, root_node)
            repName = XMLAttribValue(tag, RepnameKey)
            repiid = XMLAttribValue(tag, iidKey)
            reportType = ''

            # some reports have the same name.  Add the ID to ensure uniqueness
            repName = repName + '_' + repiid
            repName = repName.replace("/", "_")
            if 'old' not in folderName.lower():
                templateName = XMLAttribValue(tag, templateKey)
                    #print('singleColumn')
                    #Text = 'singleColumn'
                #manual entries
                if templateName == "Manual Entries by Events" or templateName == "Targets Manual Entries Per" or templateName == "Log-Sheet Single" or templateName == "AMSO4-OpLog":
                    ManualEntriesbyEvent(tag,data)
                    reportType = 'Manual'
                    #Text = 'singleColumn'
                    #print('manual')
                elif templateName == "Production Report":
                    ProductionReport(tag, data)
                    reportType = 'ProdReport'
                elif 'Trend' in templateName:
                    Trends(tag,data)
                    reportType = 'trend'
                # One Tag per column
                else:
                    SingleColumn(tag, data)
                    reportType = 'Single Column'
                global ReportList
                ReportList = ReportList.append({'Report Name': repName, 'Type': reportType, 'Folder': folderPath}, ignore_index=True)


        pass

    except Exception as ex:
        print('Error with {0} - {1}'.format(path, ex))
        pass


def getFlowSheets (root,KMTagList):
    Filter = './/FOLDERITEM[@' + foldernameKey + '="Overviews"]'
    FolderElements = root.findall(Filter)
    for FolderElement in FolderElements:

        iid = XMLAttribValue(FolderElement,iidKey)
        Filter2 = './/REPORT30[@' + parentidKey + '="' + iid + '"]'
        ReportElements = root.findall(Filter2)
        for ReportElement in ReportElements:
            repName = XMLAttribValue(ReportElement, RepnameKey)
            repiid = XMLAttribValue(ReportElement, iidKey)

            # some reports have the same name.  Add the ID to ensure uniqueness
            repName = repName + '_' + repiid
            repName = repName.replace("/", "_")

            if repName != '' and 'OLD' not in repName.upper():
                #pd.DataFrame(columns = ['Report Name', 'Tag', 'Report Type'])
                rowDataItems = pd.DataFrame(columns=['KM Tag', 'AVEVA Tag'])
                print(repName)
                LogElements = ReportElement.findall('.//PROPERTY[@' + valuerepitemKey + ']')
                for LogElement in LogElements:

                    TagName = XMLAttribValue(LogElement,valuerepitemKey)
                    AVEVATag = KMTagLookup(KMTagList,TagName)
                    if AVEVATag == '':
                        LogTagErrors(repName,TagName)
                    CalcTagReferences(TagName, repName, 'Flow Sheet')
                    CHAN9References(TagName, repName, 'Flow Sheet')
                    #{'Report Name': ReportName, 'Tag': Tag, 'Report Type': Type},ignore_index = True
                    rowDataItems = rowDataItems.append({'KM Tag': TagName,'AVEVA Tag': AVEVATag}, ignore_index = True)
                rowDataItems.to_csv(ProcessingFolder + "" + repName + ".csv")


def Trends(Report, KMTagList):
    global ErrorTagsNotinLog
    repName = XMLAttribValue(Report, RepnameKey)
    repiid = XMLAttribValue(Report, iidKey)

    #some reports have the same name.  Add the ID to ensure uniqueness
    repName = repName + '_' + repiid
    repName = repName.replace("/", "_")


    if repName != '' and 'OLD' not in repName.upper():
        filter = './/ELEMENTS[@' + keyKey + '= "ROWS"]'
        rowsElement = Report.find(filter)
        rowDataItems = []
        print(repName)
        LogItemPropertyList = Report.findall('.//PROPERTY[@' + valuerepitemKey + ']')
        for Log in LogItemPropertyList:
            Tag = Log.attrib[valuerepitemKey]
            if "'" in Tag:
                print("Single Quote")
            KMTagLookup(KMTagList,Tag)
            AVEVATag = KMTagLookup(KMTagList, Tag)
            if AVEVATag == '':
                LogTagErrors(repName, Tag)
            CalcTagReferences(Tag,repName,'Trend')
            CHAN9References(Tag, repName, 'Trend')

def ProductionReport(Report, KMTagList):
    global ErrorTagsNotinLog
    repName = XMLAttribValue(Report, RepnameKey)
    repiid = XMLAttribValue(Report, iidKey)

    #some reports have the same name.  Add the ID to ensure uniqueness
    repName = repName + '_' + repiid
    repName = repName.replace("/", "_")


    if repName != '' and 'OLD' not in repName.upper():

        rowDataItems = []
        print(repName)

        filter = './/ELEMENTS[@' + keyKey + '= "ROWS"]'
        rowsElement = Report.find(filter)
        if rowsElement is None:
            SingleColumn(Report,KMTagList)
            return
        #rowsFilter = './ELEMENT[@' + keyKey + '= "ROW1"]'
        rowsFilter = './ELEMENT'
        rowElements = rowsElement.findall(rowsFilter)
        for row in rowElements:
            rowData = []
            cellElements = row.findall('.//ELEMENT')
            for cell in cellElements:
                #print(XMLAttribValue(cell,keyKey))
                properties = cell.find('./PROPERTIES')
                LogElement = properties.find('.//PROPERTY[@' + valuerepitemKey + ']')
                if LogElement is not None:
                    #print(LogElement)
                    LogName = XMLAttribValue(LogElement, valuerepitemKey)
                    AVEVATag = KMTagLookup(KMTagList,LogName)
                    CalcTagReferences(LogName, repName, 'Production Report')
                    CHAN9References(LogName, repName, 'Production Report')

                    channelName = SignalChannelFromLog(LogName)
                    if AVEVATag == '':
                        LogTagErrors(repName, LogName)
                        rowData.append(AVEVATag)
                    else:
                        rowData.append(LogName)
                else:
                    HTMLElement = properties.find('.//PROPERTY[@' + keyKey + '="HTMLText"]')
                    HTMLText = XMLAttribValue(HTMLElement, valueKey)
                    rowData.append(HTMLText)
            if len(rowData) != 0:
                rowDataItems.append(rowData)

        if len(rowDataItems) != 0:
            csvFile = open(ProcessingFolder + "" + repName + ".csv", 'w', encoding='UTF8')
            myWriter = csv.writer(csvFile)
            myWriter.writerows(rowDataItems)
            if len(rowDataItems) > 30:
                print(repName + " has " + str(len(rowDataItems)) + " items.")
            csvFile.close()


def ManualEntriesbyEvent(Report, KMTagList):
    global ErrorTagsNotinLog
    repName = XMLAttribValue(Report,RepnameKey)
    repiid = XMLAttribValue(Report,iidKey)

    #some reports have the same name.  Add the ID to ensure uniqueness
    repName = repName + '_' + repiid

    #check for no name in the report or the word 'OLD'.  Ignore these
    if repName != '' and 'OLD' not in repName.upper():
        rowDataItems = []
        AVEVATags = ['AVEVA Tag']
        SignalNames = ['Signal Name']
        InputNames = ['Input Names']
        ChannelNames = ['Channel Names']
        SQLItems = ['SQL Lines']
        print(repName)
        repName = repName.replace("/","_")


        try:
            #find all the lines that reference signals
            SignalItemPropertyList = Report.findall('.//PROPERTY[@' + value_signalitem_signameKey + ']')

            for Property in SignalItemPropertyList:

                SignalName = Property.attrib[value_signalitem_signameKey]

                #get the parent so we can find the label that is also under this parent
                Filter = './/PROPERTY[@' + value_signalitem_signameKey + '= "' + SignalName + '"]...'
                Parent = Report.find(Filter)
                inputFilter = './/PROPERTY[@' + keyKey + '="InputName"]'
                InputProp = Parent.find(inputFilter)
                InputName = XMLAttribValue(InputProp,valueKey)

                #find the log tag that is associated with our signal name
                LogTag = LogTagFromSignalTag(SignalName)

                #lookup the AVEVA tag from the log tag
                AVEVATag = KMTagLookup(KMTagList, XMLAttribValue(LogTag,LogKeys['ritext']))



                #OMG no AVEVA tag
                if AVEVATag == '':
                    #look for the signal name in the signal XML
                    signalElement = SignalXMLLookup(SignalName)

                    #WTF that signal doesn't exist
                    if signalElement is None:
                        global Nosignals
                        Nosignals.writerow([repName, SignalName])

                    #We have a signal but can't find a log
                    elif LogTag is None:
                        global ErrorTagsNotinLog
                        ErrorTagsNotinLog.writerow([repName,'Signal - ' + SignalName])
                    #Signal exists, log exists but its not in our lookup table
                    else:
                        Row = [repName]
                        #Values = LogTag.attrib.values()
                        Row.append(XMLAttribValue(LogTag, LogKeys['ritext']))
                        Row.append(XMLAttribValue(LogTag, LogKeys['description']))
                        Row.append(XMLAttribValue(LogTag, LogKeys['valuesignal']))
                        ErrorTagsInLog.writerow(Row)

                #append info to the arrays for writing to the CSV files
                SQLExec = "exec sherritt_ManualValueInsert '[tp#EntryTime]', ['f#" + InputName + ""'], '"" + AVEVATag + "'"
                AVEVATags.append(AVEVATag)
                SignalNames.append(SignalName)
                InputNames.append(InputName)
                SQLItems.append(SQLExec)
            LogElements = Report.findall('.//PROPERTY[@' + valuerepitemKey + ']')
            for LogElement in LogElements:

                LogTag = XMLAttribValue(LogElement, valuerepitemKey)
                # get the parent so we can find the label that is also under this parent



                AVEVATag = KMTagLookup(KMTagList, LogTag)
                if AVEVATag == '':

                    Row = [repName]
                    #Values = LogTag.attrib.values()
                    Row.append(XMLAttribValue(LogElement, LogKeys['ritext']))
                    Row.append(XMLAttribValue(LogElement, LogKeys['description']))
                    Row.append(XMLAttribValue(LogElement, LogKeys['valuesignal']))
                    ErrorTagsInLog.writerow(Row)

                # append info to the arrays for writing to the CSV files

                InputName = AVEVATag.replace(".PV","")
                SQLExec = "exec sherritt_ManualValueInsert '[tp#EntryTime]', [f#" + InputName + "], '" + AVEVATag + "'"
                SQLItems.append(SQLExec)
                AVEVATags.append(AVEVATag)
                InputNames.append(InputName)




        except Exception as ex:
            print('Error {0}'.format(str(ex)))

    #write the info to CSV

        if len(AVEVATags) > 1 or len(InputNames) > 1 or len(SignalNames) > 1:

            csvFile = open(ProcessingFolder + "" + repName + ".csv", 'w', encoding='UTF8')
            myWriter = csv.writer(csvFile)
            myWriter.writerow(InputNames)
            myWriter.writerow(SignalNames)
            myWriter.writerow(AVEVATags)
            myWriter.writerow(SQLItems)
            itemCount = len(InputNames)
            if itemCount > 30:
                print(repName + " has " + str(itemCount) + " items.")

            csvFile.close()
def FolderPathFromReportXMLNode(reportNode, rootNode):
    folderNode = FindReportFolder(reportNode, rootNode)
    parentID = 0
    folderPath = ''
    #work up the folder heirarchy
    while (parentID != '1'):
        foldername = XMLAttribValue(folderNode, foldernameKey)
        folderPath = foldername + '/' + folderPath
        parentID = XMLAttribValue(folderNode, parentidKey)
        Filter = './/FOLDERITEM[@' + iidKey + '="' + parentID + '"]'
        folderNode = rootNode.find(Filter)
    return folderPath


def SingleColumn(Report, KMTagList):
    global ErrorTagsNotinLog
    repName = XMLAttribValue(Report,RepnameKey)
    repiid = XMLAttribValue(Report,iidKey)

    repName = repName + '_' + repiid

    # check for no name in the report or the word 'OLD'.  Ignore these
    if repName != '' and 'OLD' not in repName.upper():
        print(repName)
        repName = repName.replace("/","_")

        Tags = ['KM Tag']
        AVEVATags = ['AVEVA Tag']
        Channels = ['Channel Name']
        Header1 = ['Header 1']
        Header2 = ['Header 2']
        Header3 = ['Header 3']



        try:
            LogItemPropertyList = Report.findall('.//PROPERTY[@' + valuerepitemKey + ']')
        except Exception as exFind:
            print('Error finding Items in report {0}'.format(repName))

            return

        for Property in LogItemPropertyList:

            try:
                #get the tag name and append it to the tag array
                Tag = Property.attrib[valuerepitemKey]
                Tags.append(Tag)

                # get the parent so we can find the label that is also under this parent
                Filter = './/PROPERTY[@' + valuerepitemKey + '= "' + Tag + '"]...'
                Parent = Report.find(Filter)
                #Filter2 ='.//PROPERTY[@' + keyKey+'="HTMLText 1"]'

                #each column has the possibility of 3 headers.  Go get them
                HTMLText1 = HeaderLookup(Parent,'HTMLText 1')
                HTMLText2 = HeaderLookup(Parent, 'HTMLText 2')
                HTMLText3 = HeaderLookup(Parent, 'HTMLText 3')

                #lookup the aveva tag
                AVEVATag = KMTagLookup(KMTagList,Tag)
                AVEVATags.append(AVEVATag)
                channelName = SignalChannelFromLog(Tag)
                Channels.append(channelName)


                #check to see if the tag is a calc tag or if it references a chanel 9 tag (manual entry)
                CalcTagReferences(Tag, repName, 'Single Column')
                CHAN9References(Tag,repName, 'Single Column')

                #holy crap, can't find an aveva tag
                if AVEVATag == '':
                    LogTagErrors(repName,Tag)

                #append the headers to their arrays to go in the CSV
                Header1.append(HTMLText1)
                Header2.append(HTMLText2)
                Header3.append(HTMLText3)
            except Exception as ex:
                print('Error {0}'.format(str(ex)))


        #write data to the CSV and close it
        if len(Tags) > 1 or len(AVEVATags) > 1:
            csvFile = open(ProcessingFolder + "" + repName + ".csv", 'w',encoding='UTF8')
            myWriter = csv.writer(csvFile)
            myWriter.writerow(Tags)
            myWriter.writerow(AVEVATags)
            myWriter.writerow(Channels)
            myWriter.writerow(Header1)
            myWriter.writerow(Header2)
            myWriter.writerow(Header3)
            itemCount = len(Tags)
            if itemCount > 30:
                print(repName + " has " + str(itemCount) + " items.")
            csvFile.close()

def ChannelNamefromSignalName(SignalName):
    channelName = ''
    SignalElement = SignalXMLLookup(SignalName)
    channelName = XMLAttribValue(SignalElement, channameKey)
    return channelName;

#returns the signal tag channel from a log tag name
def SignalChannelFromLog(LogTagName):
    channelName = ''
    SignalElement = SignalElementFromLogTagName(LogTagName)
    channelName = XMLAttribValue(SignalElement, channameKey)

    return channelName

def SignalElementFromLogTagName(LogTagName):
    InitSignalXML()
    InitXML()
    LogTagElement = LogXMLLookup(LogTagName)
    SignalName = XMLAttribValue(LogTagElement, valuesignal_signalitem_signameKey)
    SignalElement = SignalXMLLookup(SignalName)
    return SignalElement

def CHAN9References(LogTagName,ReportName, Type):
    channelName = SignalChannelFromLog(LogTagName)

    if channelName == 'CHAN_9':
        global CHAN_9_Refs
        CHAN_9_Refs = CHAN_9_Refs.append({'Report Name': ReportName, 'Tag': LogTagName, 'Report Type': Type}, ignore_index=True)

def CalcTagReferences (Tag, ReportName, Type):
    InitSignalXML()
    InitXML()

    if Tag in Accum_Check_Tags:
        global Accum_Refs

        Accum_Refs = Accum_Refs.append({'Report Name': ReportName, 'Tag': Tag, 'Report Type': Type},ignore_index = True)

    if Tag in RHR_Check_Tags:
        global RHR_Refs
        RHR_Refs = RHR_Refs.append({'Report Name': ReportName, 'Tag': Tag, 'Report Type': Type},ignore_index = True)

    LogTagElement = LogXMLLookup(Tag)
    LogClass = XMLAttribValue(LogTagElement, LogKeys['riclass'])
    if LogClass == 'CTR':
        global CTR_Refs
        CTR_Refs = CTR_Refs.append({'Report Name': ReportName, 'Tag': Tag, 'Report Type': Type},ignore_index = True)
    for i in range(97,110):
        LogKey = LogKeys['ricode' + chr(i) + '_repitem_ritext']
        ReferncedLogName = XMLAttribValue(LogTagElement,LogKey)
        if ReferncedLogName != '':
            channelName = SignalChannelFromLog(ReferncedLogName)
            if channelName == 'CHAN_9':
                global CALC_CHAN_9_Refs

                tempdf = pd.DataFrame(columns=['Report Name', 'Tag', 'Report Type'])
                tempdf = CALC_CHAN_9_Refs[(CALC_CHAN_9_Refs['Report Name'] == ReportName) & (CALC_CHAN_9_Refs['Tag'] == Tag)]
                if tempdf.shape[0] == 0:
                    CALC_CHAN_9_Refs = CALC_CHAN_9_Refs.append({'Report Name': ReportName, 'Tag': Tag, 'Report Type': Type},ignore_index = True)
            ReferencedElement = LogXMLLookup(ReferncedLogName)
            myClass = XMLAttribValue(ReferencedElement, LogKeys['riclass'])
            if myClass == 'CTR':
                global CALC_CTR_Refs
                tempdf = CALC_CTR_Refs[(CALC_CTR_Refs['Report Name'] == ReportName) & (CALC_CTR_Refs['Tag'] == Tag)]
                if tempdf.shape[0] == 0:
                    CALC_CTR_Refs = CALC_CTR_Refs.append({'Report Name': ReportName, 'Tag': Tag, 'Report Type': Type},ignore_index = True)




#lookup the log tag in the KM lookup table
def KMTagLookup(KMTagList, Tag):

    CompareTag = Tag.replace("'","")
    AVEVATag = ''
    for row in KMTagList:
        ListTag = row[0].replace("'","")
        if ListTag == CompareTag:
            #print(row)

            AVEVATag = row[2]
    return AVEVATag

#finds the log tag which references a given signal tag
def LogTagFromSignalTag(SignalTag):

    AVEVATag = ''
    if LogXML is None:
        InitXML()
    Filter = './/REPITEM[@' + LogKeys['valuesignal_signalitem_signame'] + '="' + SignalTag + '"]'
    TagElement = LogXML.find(Filter)
    return TagElement

#loads the log tag xml file
def InitXML():
    global LogXML
    if LogXML is None:
        path = '/Users/ryanplester/WRA Dropbox/Projects/Sherritt Historian Replacement/XML export/Logs.xml'

        LogXML = ET.parse(path).getroot()

#returns the xml element from log tags xml for a given tag name
def LogXMLLookup(Tag):
    if LogXML is None:
        InitXML()

    Filter = './/REPITEM[@' + ritextKey + '="' + Tag + '"]'
    TagElement = LogXML.find(Filter)
    return TagElement


#loads the signal tag xml
def InitSignalXML():
    global SignalXML
    if SignalXML is None:
        path = '/Users/ryanplester/WRA Dropbox/Projects/Sherritt Historian Replacement/XML export/Signals.xml'

        SignalXML = ET.parse(path).getroot()

#returns the signal tag element given a tag name
def SignalXMLLookup(Tag):
    if SignalXML is None:
        InitSignalXML()

    #Filter = './/REPITEM[@' + ritextKey + '="' + Tag + '"]'
    Filter = './/SIGNALITEM[@' + signameKey + '="' + Tag + '"]'
    TagElement = SignalXML.find(Filter)
    return TagElement

#looks up the header names for the single tag/column reports
def HeaderLookup(myParent, HeaderName):
    try:
        Filter = './/PROPERTY[@' + keyKey + '="'+ HeaderName +'"]'
        HeaderObj = myParent.find(Filter)
        if HeaderObj is not None:
            return XMLAttribValue(HeaderObj,valueKey)
        else:
            return ''
    except Exception as ex:
        print('Error getting key {0} for element {1} - {2}'.format(myParent, HeaderName, ex))
        return ''


#returns an attribute from an xml element give the key
def XMLAttribValue(myElement,myKey):
    ReturnVal = ''
    try:
        if myElement is not None:
            if myKey in myElement.attrib:
                ReturnVal = myElement.attrib[myKey]
    except Exception as ex:
        print('Error getting key {0} for element {1} - {2}'.format(myElement, myKey, ex))

    return ReturnVal
def LogTagErrors(repName, logTagName):
    LogTag = LogXMLLookup(logTagName)

    # that log tag doesn't exist!  Better tell someone
    if LogTag is None:
        global ErrorTagsNotinLog
        ErrorTagsNotinLog.writerow([repName, logTagName])

    # log tag exists but not in KM lookup
    else:
        global ErrorTagsInLog
        Row = [repName]
        Values = LogTag.attrib.values()
        Row.append(XMLAttribValue(LogTag, LogKeys['ritext']))
        Row.append(XMLAttribValue(LogTag, LogKeys['description']))
        Row.append(XMLAttribValue(LogTag, LogKeys['valuesignal']))
        ErrorTagsInLog.writerow(Row)

#returns folder xml node
def FindReportFolder(reportElement, rootElement):
    parentid = XMLAttribValue(reportElement, parentidKey)
    Filter = './/FOLDERITEM[@' + iidKey + '="' + parentid + '"]'
    FolderElement = rootElement.find(Filter)
    return FolderElement


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    xmlParse()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
