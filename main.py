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

valueKey = '{http://bmi.ch.abb.com/kbs/pv}value'

ns = { 'rr' : 'http://bmi.ch.abb.com/kbs/rr', 'db' : 'http://bmi.ch.abb.com/kbs/db', 'nm' : 'http://bmi.ch.abb.com/kbs/nm' }


#{'real_person': 'http://people.example.com', 'role': 'http://characters.example.com'}

def xmlParse():
    # Use a breakpoint in the code line below to debug your script.
    files = glob.glob('/Users/ryanplester/Downloads/Sherritt_CSV/*')
    for f in files:
        os.remove(f)

    mypath = '/Users/ryanplester/WRA Dropbox/Projects/Sherritt Historian Replacement/KM reports trends XML (1)'

    for path, subdirs, files in os.walk(mypath):
        for name in files:
            if ".xml" in name:
                fullPath = os.path.join(path, name)
                print (fullPath)
                ProcessFile(fullPath)




def ProcessFile(path):
    try:
        root_node = ET.parse(path).getroot()


        for tag in root_node.findall('.//REPORT30'):
            templateName = XMLAttribValue(tag, templateKey)
            if templateName == 'Operation Rep 2 Pages' or templateName == 'Operation Rep 2 Pages' or templateName == "Operation Rep 2 Pages" or templateName == "Production Report Leach" or templateName == "Production Report Leach" or templateName == "Operation Rep 3 Page" or templateName == "Production Report Maint Rev1":
                SingleColumn(tag)

        pass
    except Exception as ex:
        print('Error with {0} - {1}'.format(path,ex))
        pass


def SingleColumn(Report):

    repName = XMLAttribValue(Report,RepnameKey)
    if repName != '':
        repName = repName.replace("/","_")
        csvFile = open("/Users/ryanplester/Downloads/Sherritt_CSV/" + repName + ".csv", 'w',encoding='UTF8')
        myWriter = csv.writer(csvFile)
        Tags = []
        Header1 = []
        Header2 = []
        Header3 = []


        try:
            LogItemPropertyList = Report.findall('.//PROPERTY[@' + valuerepitemKey + ']')
        except Exception as exFind:
            print('No Repitem found in {0}'.format(repName))
            csvFile.close()



        for Property in LogItemPropertyList:

            try:
                Tags.append(Property.attrib[valuerepitemKey])
                Filter = './/PROPERTY[@' + valuerepitemKey + '= "' +Property.attrib[valuerepitemKey] +  '"]...'
                Parent = Report.find(Filter)
                #Filter2 ='.//PROPERTY[@' + keyKey+'="HTMLText 1"]'
                HTMLText1 = HeaderLookup(Parent,'HTMLText 1')
                HTMLText2 = HeaderLookup(Parent, 'HTMLText 2')
                HTMLText3 = HeaderLookup(Parent, 'HTMLText 3')
                Header1.append(HTMLText1)
                Header2.append(HTMLText2)
                Header3.append(HTMLText3)
            except Exception as ex:
                print('Error {0}',str(ex))



        myWriter.writerow(Tags)
        myWriter.writerow(Header1)
        myWriter.writerow(Header2)
        myWriter.writerow(Header3)
        Tags.clear()
        Header1.clear()
        Header2.clear()
        Header3.clear()

        csvFile.close()
def HeaderLookup(myParent, HeaderName):
    try:
        Filter = './/PROPERTY[@' + keyKey + '="'+ HeaderName +'"]'
        HeaderObj = myParent.find(Filter)
        if HeaderObj is not None:
            return XMLAttribValue(HeaderObj,valueKey)
        else:
            return ''
    except Exception as ex:
        print('Error getting key {0} for element {1} - {2}'.format(myParent.text, HeaderName, ex))
        return ''

def XMLAttribValue(myElement,myKey):
    try:
        if myKey in myElement.attrib:
            return myElement.attrib[myKey]
        else:
            return ''
    except Exception as ex:
        print('Error getting key {0} for element {1} - {2}'.format(myElement.text, myKey, ex))
        return ''


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    xmlParse()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
