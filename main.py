# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import os
import xml.dom.minidom
import xml.etree.ElementTree as ET
import glob

templateKey = '{http://bmi.ch.abb.com/kbs/rr}tplid_template_tplname'
descKey = '{http://bmi.ch.abb.com/kbs/db}description'
keyKey = '{http://bmi.ch.abb.com/kbs/nm}key'
valuerepitemKey = '{http://bmi.ch.abb.com/kbs/rr}value_repitem_ritext'

def xmlParse():
    # Use a breakpoint in the code line below to debug your script.


    mypath = '/Users/ryanplester/WRA Dropbox/Projects/Sherritt Historian Replacement/KM reports trends XML (1)'

    for path, subdirs, files in os.walk(mypath):
        for name in files:
            if '.xml' in name:
                fullPath = os.path.join(path, name)
                print (fullPath)
                ProcessFile(fullPath)




def ProcessFile(path):
    try:
        root_node = ET.parse(path).getroot()
        print(root_node)

        for tag in root_node.iter('REPORT30'):


            if templateKey in tag.attrib:
                templateName = tag.attrib[templateKey]
                if templateName == 'Operation Rep 2 Pages' or templateName == 'Operation Rep 2 Pages':
                    SingleColumn(tag)

        pass
    except:
        print('Error with ' + path)
        pass


def SingleColumn(Report):
    if descKey in Report.attrib:
        value = Report.attrib[descKey]

        for column in Report.iter('ELEMENT'):
            if keyKey in column.attrib:
                if 'COLUMN' in column.attrib[keyKey]:
                    for Properties in column.iter('PROPERTIES'):
                        for Property in Properties.iter('PROPERTY'):
                            if valuerepitemKey in Property.attrib:
                                for Property2 in Properties.iter('PROPERTY'):
                                    print(Property2.attrib)



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    xmlParse()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
