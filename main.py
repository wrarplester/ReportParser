# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import os

import xml.etree.ElementTree as ET
import csv
import glob
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



#{'real_person': 'http://people.example.com', 'role': 'http://characters.example.com'}


#main parsing function
def xmlParse():
    # Use a breakpoint in the code line below to debug your script.
    files = glob.glob('/Users/ryanplester/Downloads/Sherritt_CSV/*')
    for f in files:
        os.remove(f)

    #init the csv files.  Global because the various report types can all write to them
    CSVFile1  = open("/Users/ryanplester/Downloads/Sherritt_CSV/@No KM Lookup In Logs.csv", 'w', encoding='UTF8')
    global ErrorTagsInLog
    ErrorTagsInLog = csv.writer(CSVFile1)
    ErrorTagsInLog.writerow(['Report Name','RItext', 'Description', 'Signal'])
    CSVFile2 = open("/Users/ryanplester/Downloads/Sherritt_CSV/@No KM Lookup No Logs.csv", 'w', encoding='UTF8')
    global ErrorTagsNotinLog
    ErrorTagsNotinLog = csv.writer(CSVFile2)
    CSVFile3 = open("/Users/ryanplester/Downloads/Sherritt_CSV/@No Signal.csv", 'w', encoding='UTF8')
    global Nosignals
    NoSignals = csv.writer(CSVFile3)
    CSVFile4 = open("/Users/ryanplester/Downloads/Sherritt_CSV/@Signals Without Logs.csv", 'w', encoding='UTF8')
    global SignalsWithoutLogs
    SignalsWithoutLogs = csv.writer(CSVFile4)
    mypath = '/Users/ryanplester/WRA Dropbox/Projects/Sherritt Historian Replacement/KM reports trends XML (1)'

    #iterate through the xml files in the path
    for path, subdirs, files in os.walk(mypath):
        for name in files:
            if ".xml" in name and "old" not in name and 'User' not in name:
                fullPath = os.path.join(path, name)
                print (fullPath)
                ProcessFile(fullPath)

    #close the error files
    CSVFile1.close()
    CSVFile2.close()
    CSVFile3.close()
    CSVFile4.close()



def ProcessFile(path):
    try:
        #get the root node and the KM tag lookup
        root_node = ET.parse(path).getroot()
        KMLookupCSV = open("/Users/ryanplester/Downloads/KM Lookup - Sheet3.csv")
        KMLookupReader = csv.reader(KMLookupCSV)
        data = list(KMLookupReader)
        KMLookupCSV.close()        
        
        for tag in root_node.findall('.//REPORT30'):
            reportFolder = FindReportFolder(tag, root_node)
            folderName = XMLAttribValue(reportFolder, foldernameKey)
            if 'Old' not in folderName:
                templateName = XMLAttribValue(tag, templateKey)
                #One Tag per column
                if templateName == 'Operation Rep 2 Pages' or templateName == 'Operation Rep 2 Pages' \
                        or templateName == "Operation Rep 2 Pages" or templateName == "Production Report Leach" \
                        or templateName == "Production Report Leach" or templateName == "Operation Rep 3 Page" \
                        or templateName == "Production Report Maint Rev1":
                    SingleColumn(tag, data)
                    #print('singleColumn')
                    #Text = 'singleColumn'
                #manual entries
                elif templateName == "Manual Entries by Events" or templateName == "Targets Manual Entries Per":
                    ManualEntriesbyEvent(tag,data)
                    #Text = 'singleColumn'
                    #print('manual')
                elif templateName == "Production Report":
                    ProductionReport(tag, data)
        pass
    except Exception as ex:
        print('Error with {0} - {1}'.format(path, ex))
        pass

def ProductionReport(Report, KMTagList):
    global ErrorTagsNotinLog
    repName = XMLAttribValue(Report, RepnameKey)
    repiid = XMLAttribValue(Report, iidKey)

    #some reports have the same name.  Add the ID to ensure uniqueness
    repName = repName + '_' + repiid
    repName = repName.replace("/", "_")


    if repName != '' and 'OLD' not in repName:

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
            csvFile = open("/Users/ryanplester/Downloads/Sherritt_CSV/" + repName + ".csv", 'w', encoding='UTF8')
            myWriter = csv.writer(csvFile)
            myWriter.writerows(rowDataItems)
            csvFile.close()


def ManualEntriesbyEvent(Report, KMTagList):
    global ErrorTagsNotinLog
    repName = XMLAttribValue(Report,RepnameKey)
    repiid = XMLAttribValue(Report,iidKey)
    
    #some reports have the same name.  Add the ID to ensure uniqueness
    repName = repName + '_' + repiid

    #check for no name in the report or the word 'OLD'.  Ignore these
    if repName != '' and 'OLD' not in repName:
        rowDataItems = []
        AVEVATags = ['AVEVA Tag']
        SignalNames = ['Signal Name']
        InputNames = ['Input Names']
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
                LogTag = LogTagFromSignalTag(KMTagList, SignalName)
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
                        Values = LogTag.attrib.values()
                        Row.append(XMLAttribValue(LogTag, LogKeys['ritext']))
                        Row.append(XMLAttribValue(LogTag, LogKeys['description']))
                        Row.append(XMLAttribValue(LogTag, LogKeys['valuesignal']))
                        ErrorTagsInLog.writerow(Row)
                
                #append info to the arrays for writing to the CSV files
                AVEVATags.append(AVEVATag)
                SignalNames.append(SignalName)
                InputNames.append(InputName)

        except Exception as ex:
            print('Error {0}'.format(str(ex)))
            
    #write the info to CSC

        if len(AVEVATags) > 1 or len(InputNames) > 1 or len(SignalNames) > 1:

            csvFile = open("/Users/ryanplester/Downloads/Sherritt_CSV/" + repName + ".csv", 'w', encoding='UTF8')
            myWriter = csv.writer(csvFile)
            myWriter.writerow(InputNames)
            myWriter.writerow(SignalNames)
            myWriter.writerow(AVEVATags)
            csvFile.close()

def SingleColumn(Report, KMTagList):
    global ErrorTagsNotinLog
    repName = XMLAttribValue(Report,RepnameKey)
    repiid = XMLAttribValue(Report,iidKey)

    repName = repName + '_' + repiid

    # check for no name in the report or the word 'OLD'.  Ignore these
    if repName != '' and 'OLD' not in repName:
        print(repName)
        repName = repName.replace("/","_")

        Tags = ['KM Tag']
        AVEVATags = ['AVEVA Tag']
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
            csvFile = open("/Users/ryanplester/Downloads/Sherritt_CSV/" + repName + ".csv", 'w',encoding='UTF8')
            myWriter = csv.writer(csvFile)
            myWriter.writerow(Tags)
            myWriter.writerow(AVEVATags)
            myWriter.writerow(Header1)
            myWriter.writerow(Header2)
            myWriter.writerow(Header3)
            csvFile.close()
        
#lookup the log tag in the KM lookup table
def KMTagLookup(KMTagList, Tag):
        
    AVEVATag = ''
    for row in KMTagList:
        if row[0] == Tag:
            #print(row)

            AVEVATag = row[2]
    return AVEVATag

#finds the log tag which references a given signal tag
def LogTagFromSignalTag(KMTagList, SignalTag):

    AVEVATag = ''
    if LogXML is None:
        InitXML()
    Filter = './/REPITEM[@' + LogKeys['valuesignal_signalitem_signame'] + '="' + SignalTag + '"]'
    TagElement = LogXML.find(Filter)
    return TagElement

#loads the log tag xml file
def InitXML():
    path = '/Users/ryanplester/WRA Dropbox/Projects/Sherritt Historian Replacement/2022-03-18 XML export/Logs.xml'
    global LogXML
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
    path = '/Users/ryanplester/WRA Dropbox/Projects/Sherritt Historian Replacement/2022-03-18 XML export/Signals.xml'
    global SignalXML
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


def FindReportFolder(reportElement, rootElement):
    parentid = XMLAttribValue(reportElement, parentidKey)
    Filter = './/FOLDERITEM[@' + iidKey + '="' + parentid + '"]'
    FolderElement = rootElement.find(Filter)
    return FolderElement


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    xmlParse()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
