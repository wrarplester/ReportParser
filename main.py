# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import os
import xml.dom.minidom
import xml.etree.ElementTree as ET
import csv
import glob

templateKey = '{http://bmi.ch.abb.com/kbs/rr}tplid_template_tplname'
descKey = '{http://bmi.ch.abb.com/kbs/db}description'
keyKey = '{http://bmi.ch.abb.com/kbs/nm}key'
RepnameKey = '{http://bmi.ch.abb.com/kbs/nm}repname'
valuerepitemKey = '{http://bmi.ch.abb.com/kbs/rr}value_repitem_ritext'
ritextKey = '{http://bmi.ch.abb.com/kbs/nm}ritext'
iidKey = '{http://bmi.ch.abb.com/kbs/ci}iid'
value_signalitem_signameKey = '{http://bmi.ch.abb.com/kbs/rr}value_signalitem_signame'
signameKey = '{http://bmi.ch.abb.com/kbs/nm}signame'

valueKey = '{http://bmi.ch.abb.com/kbs/pv}value'

ns = { 'rr' : 'http://bmi.ch.abb.com/kbs/rr', 'db' : 'http://bmi.ch.abb.com/kbs/db', 'nm' : 'http://bmi.ch.abb.com/kbs/nm' }

ErrorTagsNotinLog= None
ErrorTagsInLog = None
Nosignals = None
SignalsWithoutLogs = None

LogXML = None

SignalXML = None
TextPrefix = '{http://bmi.ch.abb.com/kbs/'
TextCI = TextPrefix + 'ci}'
TextNM = TextPrefix + 'nm}'
TextDB = TextPrefix + 'db}'
TextKY = TextPrefix + 'ky}'
TextRR = TextPrefix + 'rr}'
TextPK = TextPrefix + 'pk}'
TextPV = TextPrefix + 'pv}'
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

def xmlParse():
    # Use a breakpoint in the code line below to debug your script.
    files = glob.glob('/Users/ryanplester/Downloads/Sherritt_CSV/*')
    for f in files:
        os.remove(f)

    CSVFile1  = open("/Users/ryanplester/Downloads/Sherritt_CSV/@No KM Lookup In Logs.csv", 'w', encoding='UTF8')
    global ErrorTagsInLog
    ErrorTagsInLog = csv.writer(CSVFile1)
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


    for path, subdirs, files in os.walk(mypath):
        for name in files:
            if ".xml" in name and "old" not in name:
                fullPath = os.path.join(path, name)
                print (fullPath)
                ProcessFile(fullPath)

    CSVFile1.close()
    CSVFile2.close()



def ProcessFile(path):
    try:
        root_node = ET.parse(path).getroot()
        KMLookupCSV = open("/Users/ryanplester/Downloads/KM Lookup - Sheet1.csv")
        KMLookupReader = csv.reader(KMLookupCSV);
        data = list(KMLookupReader)
        KMLookupCSV.close()
        for tag in root_node.findall('.//REPORT30'):
            templateName = XMLAttribValue(tag, templateKey)
            #One Tag per column
            if templateName == 'Operation Rep 2 Pages' or templateName == 'Operation Rep 2 Pages' or templateName == "Operation Rep 2 Pages" or templateName == "Production Report Leach" or templateName == "Production Report Leach" or templateName == "Operation Rep 3 Page" or templateName == "Production Report Maint Rev1":
                SingleColumn(tag, data)
                #print('singleColumn')
            elif templateName == "Manual Entries by Events":
                ManualEntriesbyEvent(tag,data)



        pass
    except Exception as ex:
        print('Error with {0} - {1}'.format(path,ex))
        pass

def ManualEntriesbyEvent(Report, KMTagList):
    global ErrorTagsNotinLog
    repName = XMLAttribValue(Report,RepnameKey)
    repiid = XMLAttribValue(Report,iidKey)
    repName = repName + '_' + repiid

    if repName != '' and 'OLD' not in repName:
        AVEVATags = ['AVEVA Tag']
        SignalNames = ['Signal Name']
        InputNames = ['Input Names']
        print(repName)
        repName = repName.replace("/","_")
        csvFile = open("/Users/ryanplester/Downloads/Sherritt_CSV/" + repName + ".csv", 'w', encoding='UTF8')
        myWriter = csv.writer(csvFile)
        try:
            SignalItemPropertyList = Report.findall('.//PROPERTY[@' + value_signalitem_signameKey + ']')

            for Property in SignalItemPropertyList:
                SignalName = Property.attrib[value_signalitem_signameKey]
                Filter = './/PROPERTY[@' + value_signalitem_signameKey + '= "' + SignalName + '"]...'
                Parent = Report.find(Filter)
                inputFilter = './/PROPERTY[@' + keyKey + '="InputName"]'
                InputProp = Parent.find(inputFilter)
                InputName = XMLAttribValue(InputProp,valueKey)
                LogTag = KMTagLookupSignal(KMTagList, SignalName)
                AVEVATag = KMTagLookup(KMTagList, XMLAttribValue(LogTag,LogKeys['ritext']))



                if AVEVATag == '':
                    signalElement = SignalXMLLookup(SignalName)
                    if signalElement is None:
                        global Nosignals
                        Nosignals.writerow([repName, SignalName])
                    elif LogTag is None:
                        global ErrorTagsNotinLog
                        ErrorTagsNotinLog.writerow([repName,'Signal - ' + SignalName])
                    else:
                        Row = [repName]
                        Values = LogTag.attrib.values()
                        Row.append(XMLAttribValue(LogTag, LogKeys['ritext']))
                        Row.append(XMLAttribValue(LogTag, LogKeys['description']))
                        Row.append(XMLAttribValue(LogTag, LogKeys['valuesignal']))
                        ErrorTagsInLog.writerow(Row)
                AVEVATags.append(AVEVATag)
                SignalNames.append(SignalName)
                InputNames.append(InputName)

        except Exception as ex:
            print('Error {0}'.format(str(ex)))
    myWriter.writerow(InputNames)
    myWriter.writerow(SignalNames)
    myWriter.writerow(AVEVATags)
    csvFile.close()

def SingleColumn(Report, KMTagList):
    global ErrorTagsNotinLog
    repName = XMLAttribValue(Report,RepnameKey)
    repiid = XMLAttribValue(Report,iidKey)

    repName = repName + '_' + repiid

    if repName != '' and 'OLD' not in repName:
        print(repName)
        repName = repName.replace("/","_")
        csvFile = open("/Users/ryanplester/Downloads/Sherritt_CSV/" + repName + ".csv", 'w',encoding='UTF8')
        myWriter = csv.writer(csvFile)
        Tags = ['KM Tag']
        AVEVATags = ['AVEVA Tag']
        Header1 = ['Header 1']
        Header2 = ['Header 2']
        Header3 = ['Header 3']



        try:
            LogItemPropertyList = Report.findall('.//PROPERTY[@' + valuerepitemKey + ']')
        except Exception as exFind:
            print('No Repitem found in {0}'.format(repName))
            csvFile.close()

        for Property in LogItemPropertyList:

            try:
                Tag = Property.attrib[valuerepitemKey]
                Tags.append(Tag)
                Filter = './/PROPERTY[@' + valuerepitemKey + '= "' + Tag + '"]...'
                Parent = Report.find(Filter)
                #Filter2 ='.//PROPERTY[@' + keyKey+'="HTMLText 1"]'
                HTMLText1 = HeaderLookup(Parent,'HTMLText 1')
                HTMLText2 = HeaderLookup(Parent, 'HTMLText 2')
                HTMLText3 = HeaderLookup(Parent, 'HTMLText 3')


                AVEVATag =  KMTagLookup(KMTagList,Tag)
                AVEVATags.append(AVEVATag)

                if AVEVATag == '':
                    LogTag = LogXMLLookup(Tag)

                    if LogTag is None:
                        global ErrorTagsNotinLog
                        ErrorTagsNotinLog.writerow([repName,Tag])

                    else:
                        Row = [repName]
                        Values = LogTag.attrib.values()
                        Row.append(XMLAttribValue(LogTag,LogKeys['ritext']))
                        Row.append(XMLAttribValue(LogTag, LogKeys['description']))
                        Row.append(XMLAttribValue(LogTag, LogKeys['valuesignal']))
                        ErrorTagsInLog.writerow(Row)
                    #print('No AVEVA Tag found for {0}.  Log tag {1}'.format(Tag, LogTag))

                Header1.append(HTMLText1)
                Header2.append(HTMLText2)
                Header3.append(HTMLText3)
            except Exception as ex:
                print('Error {0}'.format(str(ex)))



        myWriter.writerow(Tags)
        myWriter.writerow(AVEVATags)
        myWriter.writerow(Header1)
        myWriter.writerow(Header2)
        myWriter.writerow(Header3)
        Tags.clear()
        Header1.clear()
        Header2.clear()
        Header3.clear()
        AVEVATags.clear()

        csvFile.close()

def KMTagLookup(KMTagList, Tag):
    LogXMLLookup(Tag)
    AVEVATag = ''
    for row in KMTagList:
        if row[0] == Tag:
            #print(row)

            AVEVATag = row[2]


    return AVEVATag


def KMTagLookupSignal(KMTagList, SignalTag):

    AVEVATag = ''
    if LogXML is None:
        InitXML()
    Filter = './/REPITEM[@' + LogKeys['valuesignal_signalitem_signame'] + '="' + SignalTag + '"]'
    TagElement = LogXML.find(Filter)
    return TagElement

def InitXML():
    path = '/Users/ryanplester/WRA Dropbox/Projects/Sherritt Historian Replacement/2022-03-18 XML export/Logs.xml'
    global LogXML
    LogXML = ET.parse(path).getroot()
def LogXMLLookup(Tag):
    if LogXML is None:
        InitXML()

    #Filter = './/REPITEM[@' + ritextKey + '="' + Tag + '"]'
    Filter = './/REPITEM[@' + ritextKey + '="' + Tag + '"]'
    TagElement = LogXML.find(Filter)
    return TagElement

def InitSignalXML():
    path = '/Users/ryanplester/WRA Dropbox/Projects/Sherritt Historian Replacement/2022-03-18 XML export/Signals.xml'
    global SignalXML
    SignalXML = ET.parse(path).getroot()
def SignalXMLLookup(Tag):
    if SignalXML is None:
        InitSignalXML()

    #Filter = './/REPITEM[@' + ritextKey + '="' + Tag + '"]'
    Filter = './/SIGNALITEM[@' + signameKey + '="' + Tag + '"]'
    TagElement = SignalXML.find(Filter)
    return TagElement


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

def XMLAttribValue(myElement,myKey):
    try:
        if myElement is None:
            return ''
        elif myKey in myElement.attrib:
            return myElement.attrib[myKey]
        else:
            return ''
    except Exception as ex:
        print('Error getting key {0} for element {1} - {2}'.format(myElement, myKey, ex))
        return ''


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    xmlParse()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
